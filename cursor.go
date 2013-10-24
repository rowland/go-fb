package fb

/*
#include <ibase.h>
#include <stdlib.h>
#include <string.h>
#include "fb.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

type Cursor struct {
	connection    *Connection
	open          bool
	eof           bool
	auto_transact C.isc_tr_handle
	stmt          C.isc_stmt_handle
	i_sqlda       *C.XSQLDA
	o_sqlda       *C.XSQLDA
	i_buffer      *C.char
	i_buffer_size C.long
	o_buffer      *C.char
	o_buffer_size C.long
	Columns       []*Column
	ColumnsMap    map[string]*Column
	err           error
	row, lastRow  Row
	lastRowMap    map[string]interface{}
}

const sqlda_colsinit = 50

func newCursor(conn *Connection) (cursor *Cursor, err error) {
	var isc_status [20]C.ISC_STATUS

	if err = conn.check(); err != nil {
		return
	}
	cursor = &Cursor{connection: conn}
	cursor.i_sqlda = C.sqlda_alloc(sqlda_colsinit)
	cursor.o_sqlda = C.sqlda_alloc(sqlda_colsinit)
	C.isc_dsql_alloc_statement2(&isc_status[0], &conn.db, &cursor.stmt)
	runtime.SetFinalizer(cursor, finalize)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	return cursor, nil
}

func (cursor *Cursor) execute(sql string, args ...interface{}) (rowsAffected int, err error) {
	var isc_status [20]C.ISC_STATUS

	if cursor.open {
		C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
		cursor.open = false
	}
	if cursor.connection.TransactionStarted() {
		rowsAffected, err = cursor.execute2(sql, args...)
	} else {
		if err = cursor.connection.TransactionStart(""); err != nil {
			return
		}
		cursor.auto_transact = cursor.connection.transact
		rowsAffected, err = cursor.execute2(sql, args...)
		// fmt.Printf("rowsAffected: %d\n", rowsAffected)
		if err != nil {
			cursor.connection.Rollback()
		} else if !cursor.open {
			err = cursor.connection.Commit()
		}
	}
	if !cursor.open {
		cursor.close()
	}
	return
}

const nullTerminated = 0

func (cursor *Cursor) execute2(sql string, args ...interface{}) (rowsAffected int, err error) {
	var isc_status [20]C.ISC_STATUS

	// prepare query
	sql2 := C.CString(sql)
	defer C.free(unsafe.Pointer(sql2))
	sql3 := (*C.ISC_SCHAR)(unsafe.Pointer(sql2))
	C.isc_dsql_prepare(&isc_status[0], &cursor.connection.transact, &cursor.stmt, nullTerminated, sql3, C.SQL_DIALECT_CURRENT, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// get statement type
	isc_info_stmt := [...]C.ISC_SCHAR{C.isc_info_sql_stmt_type}
	var isc_info_buff [16]C.ISC_SCHAR
	C.isc_dsql_sql_info(&isc_status[0], &cursor.stmt,
		C.short(unsafe.Sizeof(isc_info_stmt[0])), &isc_info_stmt[0],
		C.short(unsafe.Sizeof(isc_info_buff[0])*16), &isc_info_buff[0])
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}

	var statement C.long
	if isc_info_buff[0] == C.isc_info_sql_stmt_type {
		length := C.isc_vax_integer(&isc_info_buff[1], 2)
		statement = C.long(C.isc_vax_integer(&isc_info_buff[3], C.short(length)))
	} else {
		statement = 0
	}
	// describe input parameters
	C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.i_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// describe output parameters
	C.isc_dsql_describe(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// get number of parameters and reallocate SQLDA
	in_params := cursor.i_sqlda.sqld
	if cursor.i_sqlda.sqln < in_params {
		C.free(unsafe.Pointer(cursor.i_sqlda))
		cursor.i_sqlda = C.sqlda_alloc(C.long(in_params))
		// describe again
		C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.i_sqlda)
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
	}
	// get size of parameters buffer and reallocate it
	if in_params > 0 {
		length := C.calculate_buffsize(cursor.i_sqlda)
		if length > cursor.i_buffer_size {
			cursor.i_buffer = (*C.char)(C.realloc(unsafe.Pointer(cursor.i_buffer), C.size_t(length)))
			cursor.i_buffer_size = length
		}
	}
	if cursor.o_sqlda.sqld != 0 {
		// open cursor if statement is query
		// get number of columns and reallocate SQLDA
		cols := cursor.o_sqlda.sqld
		if cursor.o_sqlda.sqln < cols {
			C.free(unsafe.Pointer(cursor.o_sqlda))
			cursor.o_sqlda = C.sqlda_alloc(C.long(cols))
			// describe again
			C.isc_dsql_describe(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.o_sqlda)
			if err = fbErrorCheck(&isc_status); err != nil {
				return
			}
		}

		var i_sqlda *C.XSQLDA
		if in_params > 0 {
			if err = cursor.setInputParams(args); err != nil {
				return
			}
			i_sqlda = cursor.i_sqlda
		} else {
			i_sqlda = (*C.XSQLDA)(nil)
		}

		// open cursor
		C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, i_sqlda, (*C.XSQLDA)(nil))
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
		cursor.open = true

		// get size of results buffer and reallocate it
		length := C.calculate_buffsize(cursor.o_sqlda)
		if length > cursor.o_buffer_size {
			cursor.o_buffer = (*C.char)(C.realloc(unsafe.Pointer(cursor.o_buffer), C.size_t(length)))
			cursor.o_buffer_size = length
		}

		// Set the description attributes
		cursor.Columns = columnsFromSqlda(cursor.o_sqlda, cursor.connection.database.LowercaseNames)
		cursor.ColumnsMap = columnsMapFromSlice(cursor.Columns)
	} else {
		// execute statement if not query
		if statement == C.isc_info_sql_stmt_start_trans {
			panic("use fb.Connection.Transaction()")
		} else if statement == C.isc_info_sql_stmt_commit {
			panic("use fb.Connection.Commit()")
		} else if statement == C.isc_info_sql_stmt_rollback {
			panic("use fb.Connection.Rollback()")
		} else if in_params > 0 {
			if err = cursor.setInputParams(args); err != nil {
				return
			}
			C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, cursor.i_sqlda, (*C.XSQLDA)(nil))
			if err = fbErrorCheck(&isc_status); err != nil {
				return
			}
		} else {
			C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, (*C.XSQLDA)(nil), (*C.XSQLDA)(nil))
			if err = fbErrorCheck(&isc_status); err != nil {
				return
			}
		}
		rowsAffected, err = cursor.rowsAffected(statement)
	}
	return
}

func (cursor *Cursor) finalize() {
	if cursor.i_sqlda != nil {
		C.free(unsafe.Pointer(cursor.i_sqlda))
		cursor.i_sqlda = nil
	}
	if cursor.o_sqlda != nil {
		C.free(unsafe.Pointer(cursor.o_sqlda))
		cursor.o_sqlda = nil
	}
	if cursor.i_buffer != nil {
		C.free(unsafe.Pointer(cursor.i_buffer))
		cursor.i_buffer = nil
	}
	if cursor.o_buffer != nil {
		C.free(unsafe.Pointer(cursor.o_buffer))
		cursor.o_buffer = nil
	}
}

func finalize(cursor *Cursor) {
	if cursor.i_sqlda != nil {
		C.free(unsafe.Pointer(cursor.i_sqlda))
		cursor.i_sqlda = nil
	}
	if cursor.o_sqlda != nil {
		C.free(unsafe.Pointer(cursor.o_sqlda))
		cursor.o_sqlda = nil
	}
	if cursor.i_buffer != nil {
		C.free(unsafe.Pointer(cursor.i_buffer))
		cursor.i_buffer = nil
	}
	if cursor.o_buffer != nil {
		C.free(unsafe.Pointer(cursor.o_buffer))
		cursor.o_buffer = nil
	}
}

func (cursor *Cursor) setInputParams(args []interface{}) (err error) {
	if int(cursor.i_sqlda.sqld) != len(args) {
		return errors.New(fmt.Sprintf("statement requires %d items; %d given", cursor.i_sqlda.sqld, len(args)))
	}
	offset := C.ISC_SHORT(0)
	for count, arg := range args {
		ivar := C.sqlda_sqlvar(cursor.i_sqlda, C.ISC_SHORT(count))
		if argi, ok := arg.(Interfacer); ok {
			if argi.Interface() == nil {
				arg = nil
			}
		}
		if arg != nil {
			dtp := ivar.sqltype & ^1 // erase null flag
			alignment := ivar.sqllen

			switch dtp {

			case C.SQL_TEXT:
				alignment = 1
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var svalue string
				if svalue, err = stringFromIf(arg); err != nil {
					return
				}
				if len(svalue) > int(ivar.sqllen) {
					return fmt.Errorf("CHAR overflow: %d bytes exceeds %d byte(s) allowed.", len(svalue), ivar.sqllen)
				}
				csvalue := C.CString(svalue)
				defer C.free(unsafe.Pointer(csvalue))
				C.memcpy(unsafe.Pointer(ivar.sqldata), unsafe.Pointer(csvalue), C.size_t(len(svalue)))
				ivar.sqllen = C.ISC_SHORT(len(svalue))
				offset += ivar.sqllen + 1

			case C.SQL_VARYING:
				alignment = C.SHORT_SIZE
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				vary := (*C.VARY)(unsafe.Pointer(ivar.sqldata))
				var svalue string
				if svalue, err = stringFromIf(arg); err != nil {
					return
				}
				if len(svalue) > int(ivar.sqllen) {
					return fmt.Errorf("VARCHAR overflow: %d bytes exceeds %d byte(s) allowed.", len(svalue), ivar.sqllen)
				}
				csvalue := C.CString(svalue)
				defer C.free(unsafe.Pointer(csvalue))
				C.memcpy(unsafe.Pointer(&vary.vary_string), unsafe.Pointer(csvalue), C.size_t(len(svalue)))
				vary.vary_length = C.short(len(svalue))
				offset += C.ISC_SHORT(vary.vary_length) + C.SHORT_SIZE

			case C.SQL_SHORT:
				// fmt.Println("Insert SQL_SHORT")
				var lvalue C.ISC_LONG
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				if ivar.sqlscale < 0 {
					var dvalue float64
					dvalue, err = float64FromIf(arg)
					if err != nil {
						return
					}
					dvalue *= math.Pow10(-int(ivar.sqlscale))
					lvalue = C.ISC_LONG(int64(dvalue))
				} else {
					var ivalue int64
					ivalue, err = int64FromIf(arg)
					if err != nil {
						return
					}
					lvalue = C.ISC_LONG(ivalue)
				}
				if lvalue < -32768 || lvalue > 32767 {
					return errors.New("short integer overflow")
				}
				*(*C.ISC_LONG)(unsafe.Pointer(ivar.sqldata)) = lvalue
				offset += alignment

			case C.SQL_LONG:
				// fmt.Println("Insert SQL_LONG")
				var lvalue C.ISC_LONG
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				if ivar.sqlscale < 0 {
					var dvalue float64
					dvalue, err = float64FromIf(arg)
					if err != nil {
						return
					}
					dvalue *= math.Pow10(-int(ivar.sqlscale))
					lvalue = C.ISC_LONG(int64(dvalue))
				} else {
					var ivalue int64
					ivalue, err = int64FromIf(arg)
					if err != nil {
						return
					}
					lvalue = C.ISC_LONG(ivalue)
				}
				if lvalue < -2147483647 || lvalue > 2147483647 {
					return errors.New("integer overflow")
				}
				*(*C.ISC_LONG)(unsafe.Pointer(ivar.sqldata)) = lvalue
				offset += alignment

			case C.SQL_FLOAT:
				// fmt.Println("Insert SQL_FLOAT")
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var dvalue float64
				dvalue, err = float64FromIf(arg)
				if err != nil {
					return
				}
				var dcheck float64
				if dvalue >= 0.0 {
					dcheck = dvalue
				} else {
					dcheck = dvalue * -1
				}
				if dcheck != 0.0 && (dcheck < math.SmallestNonzeroFloat32 || dcheck > math.MaxFloat32) {
					return errors.New("float overflow")
				}

				*(*float32)(unsafe.Pointer(ivar.sqldata)) = float32(dvalue)
				offset += alignment

			case C.SQL_DOUBLE:
				// fmt.Println("Insert SQL_DOUBLE")
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var dvalue float64
				dvalue, err = float64FromIf(arg)
				if err != nil {
					return
				}

				*(*float64)(unsafe.Pointer(ivar.sqldata)) = dvalue
				offset += alignment

			case C.SQL_INT64:
				// fmt.Println("Insert SQL_INT64")
				var llvalue C.ISC_INT64
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))

				if ivar.sqlscale < 0 {
					var dvalue float64
					dvalue, err = float64FromIf(arg)
					if err != nil {
						return
					}
					dvalue *= math.Pow10(-int(ivar.sqlscale))
					llvalue = C.ISC_INT64(int64(dvalue))
				} else {
					var ivalue int64
					ivalue, err = int64FromIf(arg)
					if err != nil {
						return
					}
					llvalue = C.ISC_INT64(ivalue)
				}
				*(*C.ISC_INT64)(unsafe.Pointer(ivar.sqldata)) = llvalue
				offset += alignment

			case C.SQL_BLOB:
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))

				var bs []byte
				bs, err = bytesFromIf(arg)
				if err != nil {
					return
				}

				var blobHandle C.isc_blob_handle = 0
				var blobId C.ISC_QUAD
				var isc_status [20]C.ISC_STATUS

				C.isc_create_blob2(
					&isc_status[0], &cursor.connection.db, &cursor.connection.transact,
					&blobHandle, &blobId, 0, (*C.ISC_SCHAR)(nil))
				if err = fbErrorCheck(&isc_status); err != nil {
					return
				}
				length := len(bs)
				i := 0
				for length >= 4096 && err == nil {
					C.isc_put_segment(&isc_status[0], &blobHandle, 4096, (*C.ISC_SCHAR)(unsafe.Pointer(&bs[i])))
					err = fbErrorCheck(&isc_status)
					i += 4096
					length -= 4096
				}
				if length > 0 && err == nil {
					C.isc_put_segment(&isc_status[0], &blobHandle, C.ushort(length), (*C.ISC_SCHAR)(unsafe.Pointer(&bs[i])))
					err = fbErrorCheck(&isc_status)
				}
				if err != nil {
					return
				}
				C.isc_close_blob(&isc_status[0], &blobHandle)
				if err = fbErrorCheck(&isc_status); err != nil {
					return
				}

				*(*C.ISC_QUAD)(unsafe.Pointer(ivar.sqldata)) = blobId
				offset += alignment

			case C.SQL_TIMESTAMP:
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var tvalue time.Time
				tvalue, err = timeFromIf(arg, cursor.connection.Location)
				if err != nil {
					return
				}
				isc_ts := timestampFromTime(tvalue, cursor.connection.Location)
				*(*C.ISC_TIMESTAMP)(unsafe.Pointer(ivar.sqldata)) = isc_ts
				offset += alignment

			case C.SQL_TYPE_TIME:
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var tvalue time.Time
				tvalue, err = timeFromIf(arg, cursor.connection.Location)
				if err != nil {
					return
				}
				isc_ts := iscTimeFromTime(tvalue, cursor.connection.Location)
				*(*C.ISC_TIME)(unsafe.Pointer(ivar.sqldata)) = isc_ts
				offset += alignment

			case C.SQL_TYPE_DATE:
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				var tvalue time.Time
				tvalue, err = timeFromIf(arg, cursor.connection.Location)
				if err != nil {
					return
				}
				isc_ts := timestampFromTime(tvalue, cursor.connection.Location)
				*(*C.ISC_TIMESTAMP)(unsafe.Pointer(ivar.sqldata)) = isc_ts
				offset += alignment
			default:
				panic("Shouldn't reach here! (dtp not implemented)")
			}

			if (ivar.sqltype & 1) != 0 {
				offset = fbAlign(offset, C.SHORT_SIZE)
				ivar.sqlind = (*C.ISC_SHORT)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				*ivar.sqlind = 0
				offset += C.SHORT_SIZE
			}
		} else if (ivar.sqltype & 1) != 0 {
			ivar.sqldata = (*C.ISC_SCHAR)(nil)
			offset = fbAlign(offset, C.SHORT_SIZE)
			ivar.sqlind = (*C.ISC_SHORT)(unsafe.Pointer((uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset))))
			*ivar.sqlind = -1
			offset += C.SHORT_SIZE
			// fmt.Printf("NULL %d: %v\n", count, arg)
		} else {
			return errors.New("specified column is not permitted to be null")
		}
	}
	return nil
}

func (cursor *Cursor) executeWithParams(args []interface{}) (err error) {
	var isc_status [20]C.ISC_STATUS

	if err = cursor.setInputParams(args); err != nil {
		return
	}
	C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, cursor.i_sqlda, (*C.XSQLDA)(nil))
	return fbErrorCheck(&isc_status)
}

func (cursor *Cursor) fbCursorDrop() (err error) {
	var isc_status [20]C.ISC_STATUS

	if cursor.open {
		C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
	}
	C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_drop)
	return fbErrorCheck(&isc_status)
}

func (cursor *Cursor) drop() (err error) {
	err = cursor.fbCursorDrop()
	return
}

func (cursor *Cursor) rowsAffected(statementType C.long) (int, error) {
	inserted, selected, updated, deleted := 0, 0, 0, 0
	var request = [...]C.ISC_SCHAR{C.isc_info_sql_records}
	var response [64]C.ISC_SCHAR
	var isc_status [20]C.ISC_STATUS

	C.isc_dsql_sql_info(
		&isc_status[0], &cursor.stmt, C.short(unsafe.Sizeof(request)),
		&request[0], C.short(unsafe.Sizeof(response)), &response[0])
	if err := fbErrorCheck(&isc_status); err != nil {
		return 0, err
	}
	if response[0] != C.isc_info_sql_records {
		return -1, nil
	}
	r := 3 // skip past first cluster
	for response[r] != C.isc_info_end {
		countType := response[r]
		r++
		len := C.short(C.isc_vax_integer(&response[r], C.SHORT_SIZE))
		r += C.SHORT_SIZE
		switch countType {
		case C.isc_info_req_insert_count:
			inserted = int(C.isc_vax_integer(&response[r], len))
		case C.isc_info_req_select_count:
			selected = int(C.isc_vax_integer(&response[r], len))
		case C.isc_info_req_update_count:
			updated = int(C.isc_vax_integer(&response[r], len))
		case C.isc_info_req_delete_count:
			deleted = int(C.isc_vax_integer(&response[r], len))
		}
		r += int(len)
	}
	switch statementType {
	case C.isc_info_sql_stmt_select:
		return selected, nil
	case C.isc_info_sql_stmt_insert:
		return inserted, nil
	case C.isc_info_sql_stmt_update:
		return updated, nil
	case C.isc_info_sql_stmt_delete:
		return deleted, nil
	default:
		return inserted + selected + updated + deleted, nil
	}
}

func columnsFromSqlda(sqlda *C.XSQLDA, lowercaseNames bool) []*Column {
	cols := sqlda.sqld
	if cols == 0 {
		return nil
	}

	ary := make([]*Column, cols)
	for count := C.ISC_SHORT(0); count < cols; count++ {
		var col Column

		sqlvar := C.sqlda_sqlvar(sqlda, count)
		dtp := sqlvar.sqltype & ^1

		if sqlvar.aliasname_length > 0 {
			col.Name = C.GoStringN((*C.char)(unsafe.Pointer(&sqlvar.aliasname[0])), C.int(sqlvar.aliasname_length))
		} else {
			col.Name = C.GoStringN((*C.char)(unsafe.Pointer(&sqlvar.sqlname[0])), C.int(sqlvar.sqlname_length))
		}
		if lowercaseNames && !hasLowercase(col.Name) {
			col.Name = strings.ToLower(col.Name)
		}
		col.TypeCode = int(sqlvar.sqltype & ^1)
		col.SqlType = sqlTypeFromCode(int(dtp), int(sqlvar.sqlsubtype))
		col.SqlSubtype = NullableInt16{int16(sqlvar.sqlsubtype), false}
		col.Length = int16(sqlvar.sqllen)
		if dtp == C.SQL_VARYING {
			col.InternalSize = int(sqlvar.sqllen + C.SHORT_SIZE)
		} else {
			col.InternalSize = int(sqlvar.sqllen)
		}
		col.Precision = NullableInt16{precisionFromSqlvar(sqlvar), false}
		col.Scale = int16(sqlvar.sqlscale)
		col.Nullable = NullableBool{(sqlvar.sqltype & 1) != 0, false}

		ary[count] = &col
	}
	return ary
}

func (cursor *Cursor) check() error {
	if cursor.stmt == 0 {
		return &Error{Message: "dropped cursor"}
	}
	if !cursor.open {
		return &Error{Message: "closed cursor"}
	}
	return nil
}

func (cursor *Cursor) Close() (err error) {
	if err = cursor.check(); err != nil {
		return
	}
	return cursor.close()
}

func (cursor *Cursor) close() (err error) {
	var isc_status [20]C.ISC_STATUS

	C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
	if err = fbErrorCheckWarn(&isc_status); err != nil {
		return
	}
	C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_drop)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	cursor.open = false
	if cursor.connection.transact == cursor.auto_transact {
		err = cursor.connection.Commit()
		cursor.auto_transact = cursor.connection.transact
	}
	cursor.Columns = nil
	cursor.ColumnsMap = nil
	return
}

func (cursor *Cursor) prep() (err error) {
	var isc_status [20]C.ISC_STATUS

	if err = cursor.check(); err != nil {
		return
	}
	if err = cursor.connection.check(); err != nil {
		return
	}
	C.isc_dsql_describe(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	cols := cursor.o_sqlda.sqld
	var offset C.ISC_SHORT = 0
	for count := C.ISC_SHORT(0); count < cols; count++ {
		ovar := C.sqlda_sqlvar(cursor.o_sqlda, count)
		length, alignment := ovar.sqllen, ovar.sqllen
		dtp := ovar.sqltype & ^1
		if dtp == C.SQL_TEXT {
			alignment = 1
		} else if dtp == C.SQL_VARYING {
			length += C.SHORT_SIZE
			alignment = C.SHORT_SIZE
		}
		offset = fbAlign(offset, alignment)
		ovar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer((uintptr(unsafe.Pointer(cursor.o_buffer)) + uintptr(offset))))
		offset += length
		offset = fbAlign(offset, C.SHORT_SIZE)
		ovar.sqlind = (*C.ISC_SHORT)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.o_buffer)) + uintptr(offset)))
		offset += C.SHORT_SIZE
	}
	return
}

func (cursor *Cursor) Err() error {
	return cursor.err
}

var blobItemsFetch = [...]C.ISC_SCHAR{
	C.isc_info_blob_max_segment,
	C.isc_info_blob_num_segments,
	C.isc_info_blob_total_length,
}

func (cursor *Cursor) Next() bool {
	const SQLCODE_NOMORE = 100
	var isc_status [20]C.ISC_STATUS

	if cursor.err = cursor.prep(); cursor.err != nil {
		return false
	}
	if cursor.err = cursor.connection.check(); cursor.err != nil {
		return false
	}
	if cursor.eof {
		cursor.err = &Error{Message: "Cursor is past end of data."}
		return false
	}
	// fetch one row
	if C.isc_dsql_fetch(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.o_sqlda) == SQLCODE_NOMORE {
		cursor.eof = true
		cursor.err = io.EOF
		return false
	}
	if cursor.err = fbErrorCheck(&isc_status); cursor.err != nil {
		return false
	}
	// create result tuple
	cols := cursor.o_sqlda.sqld
	if len(cursor.row) < int(cols) {
		cursor.row = make(Row, cols)
	}
	cursor.lastRow, cursor.lastRowMap = nil, nil
	// set result value for each column
	for count := C.ISC_SHORT(0); count < cols; count++ {
		var val interface{}
		// sqlvar := &cursor.o_sqlda.sqlvar[count]
		sqlvar := C.sqlda_sqlvar(cursor.o_sqlda, count)
		dtp := sqlvar.sqltype & ^1

		// check if column is null
		if ((sqlvar.sqltype & 1) != 0) && (*sqlvar.sqlind < 0) {
			val = nil
			// fmt.Println("NULL")
		} else {
			// set column value to result tuple
			switch dtp {
			case C.SQL_TEXT:
				val = C.GoStringN((*C.char)(unsafe.Pointer(sqlvar.sqldata)), C.int(sqlvar.sqllen))
			case C.SQL_VARYING:
				vary := (*C.VARY)(unsafe.Pointer(sqlvar.sqldata))
				val = C.GoStringN((*C.char)(unsafe.Pointer(&vary.vary_string)), C.int(vary.vary_length))
			case C.SQL_SHORT:
				sval := *(*C.short)(unsafe.Pointer(sqlvar.sqldata))
				if sqlvar.sqlscale < 0 {
					val = float64(sval) / math.Pow10(-int(sqlvar.sqlscale))
				} else {
					val = int16(sval)
				}
			case C.SQL_LONG:
				lval := *(*C.ISC_LONG)(unsafe.Pointer(sqlvar.sqldata))
				if sqlvar.sqlscale < 0 {
					val = float64(lval) / math.Pow10(-int(sqlvar.sqlscale))
				} else {
					val = int32(lval)
				}
			case C.SQL_FLOAT:
				// fmt.Println("Fetch SQL_FLOAT")
				fval := *(*float32)(unsafe.Pointer(sqlvar.sqldata))
				val = fval
			case C.SQL_DOUBLE:
				// fmt.Println("Fetch SQL_DOUBLE")
				dval := *(*float64)(unsafe.Pointer(sqlvar.sqldata))
				val = dval
			case C.SQL_INT64:
				// fmt.Println("Fetch SQL_INT64")
				ival := *(*C.ISC_INT64)(unsafe.Pointer(sqlvar.sqldata))
				if sqlvar.sqlscale < 0 {
					val = float64(ival) / math.Pow10(-int(sqlvar.sqlscale))
				} else {
					val = int64(ival)
				}
			case C.SQL_TIMESTAMP:
				isc_ts := *(*C.ISC_TIMESTAMP)(unsafe.Pointer(sqlvar.sqldata))
				val = timeFromTimestamp(isc_ts, cursor.connection.Location)
			case C.SQL_TYPE_TIME:
				tm := *(*C.ISC_TIME)(unsafe.Pointer(sqlvar.sqldata))
				val = timeFromIscTime(tm, cursor.connection.Location)
			case C.SQL_TYPE_DATE:
				isc_dt := *(*C.ISC_DATE)(unsafe.Pointer(sqlvar.sqldata))
				val = timeFromIscDate(isc_dt, cursor.connection.Location)
			case C.SQL_BLOB:
				// fmt.Println("Fetch SQL_BLOB")
				var blobHandle C.isc_blob_handle = 0
				var blobID C.ISC_QUAD = *(*C.ISC_QUAD)(unsafe.Pointer(sqlvar.sqldata))
				C.isc_open_blob2(&isc_status[0], &cursor.connection.db, &cursor.connection.transact, &blobHandle, &blobID, 0, (*C.ISC_UCHAR)(nil))
				if cursor.err = fbErrorCheck(&isc_status); cursor.err != nil {
					return false
				}
				var blobInfo [32]C.ISC_SCHAR
				C.isc_blob_info(
					&isc_status[0], &blobHandle,
					C.short(unsafe.Sizeof(blobItemsFetch)), &blobItemsFetch[0],
					C.short(unsafe.Sizeof(blobInfo)), &blobInfo[0])
				if cursor.err = fbErrorCheck(&isc_status); cursor.err != nil {
					return false
				}
				var length C.short
				var maxSegment C.ISC_LONG = 0
				var numSegments C.ISC_LONG = 0
				var totalLength C.ISC_LONG = 0
				var actualSegLen C.ushort
				for i := 0; blobInfo[i] != C.isc_info_end; i += int(length) {
					item := blobInfo[i]
					i += 1
					length = C.short(C.isc_vax_integer(&blobInfo[i], 2))
					i += 2
					switch item {
					case C.isc_info_blob_max_segment:
						maxSegment = C.isc_vax_integer(&blobInfo[i], length)
					case C.isc_info_blob_num_segments:
						numSegments = C.isc_vax_integer(&blobInfo[i], length)
					case C.isc_info_blob_total_length:
						totalLength = C.isc_vax_integer(&blobInfo[i], length)
					}
				}
				bval := make([]byte, totalLength)
				for i := 0; numSegments > 0; numSegments-- {
					C.isc_get_segment(
						&isc_status[0], &blobHandle, &actualSegLen,
						C.ushort(maxSegment), (*C.ISC_SCHAR)(unsafe.Pointer(&bval[i])))
					if cursor.err = fbErrorCheck(&isc_status); cursor.err != nil {
						return false
					}
					i += int(actualSegLen)
				}
				C.isc_close_blob(&isc_status[0], &blobHandle)
				if cursor.err = fbErrorCheck(&isc_status); cursor.err != nil {
					return false
				}
				if cursor.Columns[count].SqlSubtype.Value == 1 {
					val = string(bval)
				} else {
					val = bval
				}
			}
		}
		cursor.row[count] = val
	}
	return true
}

func (cursor *Cursor) Row() Row {
	if cursor.lastRow == nil {
		cursor.lastRow = make(Row, len(cursor.Columns))
		copy(cursor.lastRow, cursor.row)
	}
	return cursor.lastRow
}

func (cursor *Cursor) RowMap() map[string]interface{} {
	if cursor.lastRowMap == nil {
		cursor.lastRowMap = make(map[string]interface{}, len(cursor.Columns))
		for i, col := range cursor.Columns {
			cursor.lastRowMap[col.Name] = cursor.row[i]
		}
	}
	return cursor.lastRowMap
}

func (cursor *Cursor) Scan(dest ...interface{}) error {
	if cursor.err != nil {
		return cursor.err
	}
	if cursor.row == nil {
		return errors.New("fb: Scan called without calling Next")
	}
	return cursor.row.Scan(dest...)
}
