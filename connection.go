package fb

/*
#cgo darwin CFLAGS: -D_XOPEN_SOURCE -D_DARWIN_C_SOURCE -I/Library/Frameworks/Firebird.framework/Headers
#cgo darwin LDFLAGS: -L. -arch x86_64 -framework Firebird
#cgo !darwin CFLAGS: -I/usr/include
#cgo !darwin LDFLAGS: -lfbclient

#include "fb.h"
#include <ibase.h>
#include <stdlib.h>

ISC_STATUS isc_start_transaction2(ISC_STATUS* isc_status,
	isc_tr_handle* tr_handle,
	short n, isc_db_handle *db, long tpb_len, char *tpb) {
	return isc_start_transaction(isc_status, tr_handle, n, db, tpb_len, tpb);
}
*/
import "C"

import (
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
	"unsafe"
)

type Connection struct {
	database     *Database
	db           C.isc_db_handle
	transact     C.isc_tr_handle
	dialect      C.ushort
	db_dialect   C.ushort
	dropped      bool
	rowsAffected int
	Location     *time.Location
}

func (conn *Connection) check() error {
	if conn.db == 0 {
		return &Error{0, "closed db connection"}
	}
	return nil
}

func (conn *Connection) Close() (err error) {
	if conn.dropped {
		return
	}
	if err = conn.check(); err != nil {
		return
	}
	if err = conn.disconnect(); err != nil {
		return
	}
	return nil
}

func (conn *Connection) Columns(tableName string) (columns []*Column, err error) {
	const sqlColumns = `
		SELECT r.rdb$field_name, r.rdb$field_source, f.rdb$field_type, f.rdb$field_sub_type,
			f.rdb$field_length, f.rdb$field_precision, f.rdb$field_scale,
			COALESCE(r.rdb$default_source, f.rdb$default_source) rdb$default_source,
			COALESCE(r.rdb$null_flag, f.rdb$null_flag) rdb$null_flag
		FROM rdb$relation_fields r
		JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
		WHERE r.rdb$relation_name = ?
		ORDER BY r.rdb$field_position`

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlColumns, tableName); err != nil {
		return
	}
	defer cursor.Close()

	for cursor.Next() {
		var col Column
		var sqlType int16
		if err = cursor.Scan(
			&col.Name,
			&col.Domain,
			&sqlType,
			&col.SqlSubtype,
			&col.Length,
			&col.Precision,
			&col.Scale,
			&col.Default,
			&col.Nullable); err != nil {
			return
		}
		col.Name = strings.TrimRightFunc(col.Name, unicode.IsSpace)
		if conn.database.LowercaseNames && !hasLowercase(col.Name) {
			col.Name = strings.ToLower(col.Name)
		}
		col.Domain = strings.TrimRightFunc(col.Domain, unicode.IsSpace)
		if strings.HasPrefix(col.Domain, "RDB$") {
			col.Domain = ""
		}
		col.SqlType = sqlTypeFromCode(int(sqlType), int(col.SqlSubtype.Value))
		if !col.Default.Null {
			col.Default.Value = strings.Replace(col.Default.Value, "DEFAULT ", "", 1)
			col.Default.Value = strings.TrimLeftFunc(col.Default.Value, unicode.IsSpace)
		}
		columns = append(columns, &col)
	}
	if cursor.Err() != io.EOF {
		err = cursor.Err()
	}
	return
}

func (conn *Connection) Commit() (err error) {
	var isc_status [20]C.ISC_STATUS

	if conn.transact != 0 {
		C.isc_commit_transaction(&isc_status[0], &conn.transact)
		err = fbErrorCheck(&isc_status)
	}
	return nil
}

func (conn *Connection) disconnect() (err error) {
	var isc_status [20]C.ISC_STATUS

	if conn.transact != 0 {
		C.isc_commit_transaction(&isc_status[0], &conn.transact)
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
	}
	if conn.dropped {
		C.isc_drop_database(&isc_status[0], &conn.db)
	} else {
		C.isc_detach_database(&isc_status[0], &conn.db)
	}
	return fbErrorCheck(&isc_status)
}

func (conn *Connection) Drop() (err error) {
	conn.dropped = true
	if err = conn.disconnect(); err != nil {
		return
	}
	return nil
}

func (conn *Connection) Execute(sql string, args ...interface{}) (cursor *Cursor, err error) {
	cursor, err = newCursor(conn)
	if err != nil {
		return
	}
	rowsAffected, err := cursor.execute(sql, args...)
	if rowsAffected >= 0 {
		conn.rowsAffected = rowsAffected
	}
	if !cursor.open {
		cursor = nil
	}
	return
}

func (conn *Connection) ExecuteScript(sql string) (err error) {
	// TODO: handle "set term"
	script := strings.Split(sql, ";")
	for _, stmt := range script {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		_, err = conn.Execute(stmt)
		if err != nil {
			return
		}
	}
	return
}

func (conn *Connection) GeneratorNames() (names []string, err error) {
	const sql = `SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS 
		WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG <> 1) 
		ORDER BY RDB$GENERATOR_NAME`
	return conn.names(sql)
}

func (conn *Connection) IndexColumns(indexName string) (names []string, err error) {
	const sql = `SELECT RDB$FIELD_NAME
		FROM RDB$INDEX_SEGMENTS 
		WHERE RDB$INDEX_SEGMENTS.RDB$INDEX_NAME = ? 
		ORDER BY RDB$INDEX_SEGMENTS.RDB$FIELD_POSITION`
	return conn.names(sql, indexName)
}

func (conn *Connection) Indexes() (indexes []*Index, err error) {
	const sql = `SELECT RDB$INDICES.RDB$RELATION_NAME, RDB$INDICES.RDB$INDEX_NAME, RDB$INDICES.RDB$UNIQUE_FLAG, RDB$INDICES.RDB$INDEX_TYPE 
	FROM RDB$INDICES 
	JOIN RDB$RELATIONS ON RDB$INDICES.RDB$RELATION_NAME = RDB$RELATIONS.RDB$RELATION_NAME 
	WHERE (RDB$RELATIONS.RDB$SYSTEM_FLAG <> 1 OR RDB$RELATIONS.RDB$SYSTEM_FLAG IS NULL);`
	var cursor *Cursor
	if cursor, err = conn.Execute(sql); err != nil {
		return
	}
	defer cursor.Close()

	for cursor.Next() {
		var index Index
		if err = cursor.Scan(&index.TableName, &index.Name, &index.Unique, &index.Unique); err != nil {
			return
		}
		index.Name = strings.TrimRightFunc(index.Name, unicode.IsSpace)
		index.TableName = strings.TrimRightFunc(index.TableName, unicode.IsSpace)
		if index.Columns, err = conn.IndexColumns(index.Name); err != nil {
			return
		}
		if conn.database.LowercaseNames && !hasLowercase(index.Name) {
			index.Name = strings.ToLower(index.Name)
		}
		if conn.database.LowercaseNames && !hasLowercase(index.TableName) {
			index.TableName = strings.ToLower(index.TableName)
		}
		indexes = append(indexes, &index)
	}
	if cursor.Err() != io.EOF {
		err = cursor.Err()
	}
	return
}

func (conn *Connection) names(sql string, args ...interface{}) (names []string, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql, args...); err != nil {
		return
	}
	defer cursor.Close()

	for cursor.Next() {
		var name string
		if err = cursor.Scan(&name); err != nil {
			return
		}
		name = strings.TrimRightFunc(name, unicode.IsSpace)
		if conn.database.LowercaseNames && !hasLowercase(name) {
			name = strings.ToLower(name)
		}
		names = append(names, name)
	}
	if cursor.Err() != io.EOF {
		err = cursor.Err()
	}
	return
}

func (conn *Connection) NextSequenceValue(name string) (value int64, err error) {
	sql := fmt.Sprintf("SELECT GEN_ID(%s, 1) FROM RDB$DATABASE", name)
	var cursor *Cursor
	if cursor, err = conn.Execute(sql); err != nil {
		return
	}
	defer cursor.Close()
	if cursor.Next() {
		cursor.Scan(&value)
	}
	err = cursor.Err()
	return
}

func (conn *Connection) PrimaryKey(tableName string) (key []string, err error) {
	const sql = `
		SELECT s.rdb$field_name
		FROM rdb$indices i
			JOIN rdb$index_segments s ON i.rdb$index_name = s.rdb$index_name
			LEFT JOIN rdb$relation_constraints c ON i.rdb$index_name = c.rdb$index_name
		WHERE i.rdb$relation_name = ? and c.rdb$constraint_type = 'PRIMARY KEY'
		ORDER BY rdb$field_position;`
	return conn.names(sql, tableName)
}

func (conn *Connection) ProcedureNames() (names []string, err error) {
	const sql = "SELECT RDB$PROCEDURE_NAME FROM RDB$PROCEDURES ORDER BY RDB$PROCEDURE_NAME"
	return conn.names(sql)
}

func (conn *Connection) QueryRow(sql string, args ...interface{}) (row []interface{}, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql, args...); err != nil {
		return
	}
	defer cursor.Close()
	if cursor.Next() {
		row = cursor.Row()
	}
	err = cursor.Err()
	return
}

func (conn *Connection) QueryRowMap(sql string, args ...interface{}) (row map[string]interface{}, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql, args...); err != nil {
		return
	}
	defer cursor.Close()
	if cursor.Next() {
		row = cursor.RowMap()
	}
	err = cursor.Err()
	return
}

func (conn *Connection) QueryRowMaps(sql string, args ...interface{}) (rows []map[string]interface{}, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql, args...); err != nil {
		return
	}
	defer cursor.Close()
	for cursor.Next() {
		rows = append(rows, cursor.RowMap())
	}
	if cursor.Err() != io.EOF {
		err = cursor.Err()
	}
	return
}

func (conn *Connection) QueryRows(sql string, args ...interface{}) (rows [][]interface{}, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql, args...); err != nil {
		return
	}
	defer cursor.Close()
	for cursor.Next() {
		rows = append(rows, cursor.Row())
	}
	if cursor.Err() != io.EOF {
		err = cursor.Err()
	}
	return
}

func (conn *Connection) RoleNames() (names []string, err error) {
	const sql = "SELECT RDB$ROLE_NAME FROM RDB$ROLES WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$ROLE_NAME"
	return conn.names(sql)
}

func (conn *Connection) Rollback() (err error) {
	var isc_status [20]C.ISC_STATUS

	if conn.transact != 0 {
		C.isc_rollback_transaction(&isc_status[0], &conn.transact)
		err = fbErrorCheck(&isc_status)
	}
	return
}

func (conn *Connection) TableNames() (names []string, err error) {
	const sql = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL 
		ORDER BY RDB$RELATION_NAME`
	return conn.names(sql)
}

func (conn *Connection) TransactionStart(options string) error {
	var isc_status [20]C.ISC_STATUS

	if conn.TransactionStarted() {
		return &Error{Message: "A transaction has been already started"}
	}
	var tpb *C.char = (*C.char)(nil)
	var tpb_len C.long = 0
	if options != "" {
		options2 := C.CString(options)
		defer C.free(unsafe.Pointer(options2))
		tpb = C.trans_parseopts(options2, &tpb_len)
		if tpb_len < 0 {
			defer C.free(unsafe.Pointer(tpb))
			return &Error{Message: C.GoString(tpb)}
		}
	}
	C.isc_start_transaction2(&isc_status[0], &conn.transact, 1, &conn.db, tpb_len, tpb)
	C.free(unsafe.Pointer(tpb))
	return fbErrorCheck(&isc_status)
}

func (conn *Connection) TransactionStarted() bool {
	return (conn.transact != 0)
}

func (conn *Connection) TriggerNames() (names []string, err error) {
	const sql = "SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$TRIGGER_NAME"
	return conn.names(sql)
}

func (conn *Connection) ViewNames() (names []string, err error) {
	const sql = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND NOT RDB$VIEW_BLR IS NULL AND RDB$FLAGS = 1 
		ORDER BY RDB$RELATION_NAME`
	return conn.names(sql)
}
