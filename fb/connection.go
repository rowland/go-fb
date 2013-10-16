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
	cursors      []*Cursor
	rowsAffected int
	Location     *time.Location
}

func (conn *Connection) check() error {
	if conn.db == 0 {
		return &Error{0, "closed db connection"}
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

func (conn *Connection) dropCursors() {
	for _, cursor := range conn.cursors {
		cursor.drop()
	}
	conn.cursors = nil
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
	conn.dropCursors()
	return nil
}

func (conn *Connection) Drop() (err error) {
	conn.dropped = true
	if err = conn.disconnect(); err != nil {
		return
	}
	conn.dropCursors()
	return nil
}

func (conn *Connection) TransactionStarted() bool {
	return (conn.transact != 0)
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

func (conn *Connection) closeCursors() {
	for _, cursor := range conn.cursors {
		cursor.Close()
	}
	conn.cursors = nil
}

func (conn *Connection) Commit() (err error) {
	var isc_status [20]C.ISC_STATUS

	if conn.transact != 0 {
		conn.closeCursors()
		C.isc_commit_transaction(&isc_status[0], &conn.transact)
		err = fbErrorCheck(&isc_status)
	}
	return nil
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

func (conn *Connection) Rollback() (err error) {
	var isc_status [20]C.ISC_STATUS

	if conn.transact != 0 {
		conn.closeCursors()
		C.isc_rollback_transaction(&isc_status[0], &conn.transact)
		err = fbErrorCheck(&isc_status)
	}
	return
}

func (conn *Connection) names(sql string) (names []string, err error) {
	var cursor *Cursor
	if cursor, err = conn.Execute(sql); err != nil {
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

func (conn *Connection) TableNames() (names []string, err error) {
	const sql = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL 
		ORDER BY RDB$RELATION_NAME`
	return conn.names(sql)
}

func (conn *Connection) ViewNames() (names []string, err error) {
	const sql = `SELECT RDB$RELATION_NAME FROM RDB$RELATIONS 
		WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND NOT RDB$VIEW_BLR IS NULL AND RDB$FLAGS = 1 
		ORDER BY RDB$RELATION_NAME`
	return conn.names(sql)
}

func (conn *Connection) GeneratorNames() (names []string, err error) {
	const sql = `SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS 
		WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG <> 1) 
		ORDER BY RDB$GENERATOR_NAME`
	return conn.names(sql)
}

func (conn *Connection) RoleNames() (names []string, err error) {
	const sql = "SELECT RDB$ROLE_NAME FROM RDB$ROLES WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$ROLE_NAME"
	return conn.names(sql)
}
