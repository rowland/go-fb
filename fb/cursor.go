package fb

/*
#include <ibase.h>
#include <stdlib.h>
#include "fb.h"
*/
import "C"

import (
	"os"
	"unsafe"
	"strings"
	"fmt"
	"regexp"
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
	Fields        []*Field
	FieldsMap     map[string]*Field
}

const sqlda_colsinit = 50

func newCursor(conn *Connection) (cursor *Cursor, err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if err = conn.check(); err != nil {
		return
	}
	cursor = &Cursor{connection: conn}
	cursor.i_sqlda = C.sqlda_alloc(sqlda_colsinit)
	cursor.o_sqlda = C.sqlda_alloc(sqlda_colsinit)
	C.isc_dsql_alloc_statement2(&isc_status[0], &conn.db, &cursor.stmt)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	return cursor, nil
}

func (cursor *Cursor) execute(sql string, args ...interface{}) (rowsAffected int, err os.Error) {
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
		cursor.connection.transactionStart(nil)
		cursor.auto_transact = cursor.connection.transact
		rowsAffected, err = cursor.execute2(sql, args...)
		if err != nil {
			cursor.connection.Rollback()
		} else if rowsAffected < 0 {
			cursor.connection.Commit()
		}
	}
	return
}

const nullTerminated = 0

func (cursor *Cursor) execute2(sql string, args ...interface{}) (rowsAffected int, err os.Error) {
	var isc_status [20]C.ISC_STATUS

	// prepare query
	sql2 := C.CString(sql)
	defer C.free(unsafe.Pointer(sql2))
	sql3 := (*C.ISC_SCHAR)(unsafe.Pointer(sql2))
	C.isc_dsql_prepare(&isc_status[0], &cursor.connection.transact, &cursor.stmt, nullTerminated, sql3, C.SQLDA_VERSION1, cursor.o_sqlda)
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
		cursor.Fields = fieldsFromSqlda(cursor.o_sqlda, cursor.connection.database.LowercaseNames)
		cursor.FieldsMap = fieldsMapFromSlice(cursor.Fields)
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
		rowsAffected = cursor.rowsAffected(statement)
	}
	return
}

func (cursor *Cursor) setInputParams(args []interface{}) (err os.Error) {
	if int(cursor.i_sqlda.sqld) != len(args) {
		return os.NewError(fmt.Sprintf("statement requires %d items; %d given", cursor.i_sqlda.sqld, len(args)))
	}
	offset := C.ISC_SHORT(0)
	for count, arg := range args {
		ivar := C.sqlda_sqlvar(cursor.i_sqlda, C.ISC_SHORT(count))
		if arg != nil {
			dtp := ivar.sqltype & ^1 // erase null flag
			alignment := ivar.sqllen

			switch dtp {
			case C.SQL_LONG:
				var lvalue C.ISC_LONG
				offset = fbAlign(offset, alignment)
				ivar.sqldata = (*C.ISC_SCHAR)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				if ivar.sqlscale < 0 {
					ratio := 1
					for scnt := C.ISC_SHORT(0); scnt > ivar.sqlscale; scnt-- {
						ratio *= 10
					}
					var dvalue float64
					dvalue, err = float64FromIf(arg)
					if err != nil {
						return
					}
					dvalue *= float64(ratio)
					lvalue = C.ISC_LONG(dvalue + 0.5)
				} else {
					var ivalue int64
					ivalue, err = int64FromIf(arg)
					if err != nil {
						return
					}
					lvalue = C.ISC_LONG(ivalue)
				}
				if lvalue < -2147483647 || lvalue > 2147483647 {
					return os.NewError("integer overflow")
				}
				*(*C.ISC_LONG)(unsafe.Pointer(ivar.sqldata)) = lvalue
				offset += alignment
			default:
				panic("Shouldn't reach here! (dtp not implemented)")
			}

			if ivar.sqltype & 1 != 0 {
				offset = fbAlign(offset, C.SHORT_SIZE)
				ivar.sqlind = (*C.ISC_SHORT)(unsafe.Pointer(uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset)))
				*ivar.sqlind = 0
				offset += C.SHORT_SIZE
			}
		} else if ivar.sqltype&1 != 0 {
			ivar.sqldata = (*C.ISC_SCHAR)(nil)
			offset = fbAlign(offset, C.SHORT_SIZE)
			ivar.sqlind = (*C.ISC_SHORT)(unsafe.Pointer((uintptr(unsafe.Pointer(cursor.i_buffer)) + uintptr(offset))))
			*ivar.sqlind = -1

			offset += C.SHORT_SIZE
		} else {
			return os.NewError("specified column is not permitted to be null")
		}
	}
	return nil
}
/*
static void fb_cursor_set_inputparams(struct FbCursor *fb_cursor, long argc, VALUE *argv)
{
	struct FbConnection *fb_connection;
	long count;
	long offset;
	long type;
	short dtp;
	VALUE obj;
	long lvalue;
	ISC_INT64 llvalue;
	long alignment;
	double ratio;
	double dvalue;
	long scnt;
	double dcheck;
	VARY *vary;
	XSQLVAR *var;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	 // static char blob_items[] = { isc_info_blob_max_segment }; 
	 // char blob_info[16]; 
	char *p;
	long length;
	 // struct time_object *tobj; 
	struct tm tms;

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);

	 // Check the number of parameters 
	if (fb_cursor->i_sqlda->sqld != argc) {
		rb_raise(rb_eFbError, "statement requires %d items; %ld given", fb_cursor->i_sqlda->sqld, argc);
	}

	 // Get the parameters 
	for (count = 0,offset = 0; count < argc; count++) {
		obj = argv[count];

		type = TYPE(obj);

		 // Convert the data type for InterBase 
		var = &fb_cursor->i_sqlda->sqlvar[count];
		if (!NIL_P(obj)) {
			dtp = var->sqltype & ~1;	// Erase null flag
			alignment = var->sqllen;

			switch (dtp) {
				case SQL_TEXT :
					alignment = 1;
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = rb_obj_as_string(obj);
					if (RSTRING_LEN(obj) > var->sqllen) {
						rb_raise(rb_eRangeError, "CHAR overflow: %ld bytes exceeds %d byte(s) allowed.",
							RSTRING_LEN(obj), var->sqllen);
					}
					memcpy(var->sqldata, RSTRING_PTR(obj), RSTRING_LEN(obj));
					var->sqllen = RSTRING_LEN(obj);
					offset += var->sqllen + 1;
					break;

				case SQL_VARYING :
					alignment = sizeof(short);
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					vary = (VARY *)var->sqldata;
					obj = rb_obj_as_string(obj);
					if (RSTRING_LEN(obj) > var->sqllen) {
						rb_raise(rb_eRangeError, "VARCHAR overflow: %ld bytes exceeds %d byte(s) allowed.",
							RSTRING_LEN(obj), var->sqllen);
					}
					memcpy(vary->vary_string, RSTRING_PTR(obj), RSTRING_LEN(obj));
					vary->vary_length = RSTRING_LEN(obj);
					offset += vary->vary_length + sizeof(short);
					break;

				case SQL_SHORT :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--)
							ratio *= 10;
						obj = double_from_obj(obj);
						dvalue = NUM2DBL(obj) * ratio;
						lvalue = (ISC_LONG)(dvalue + 0.5);
					} else {
						obj = long_from_obj(obj);
						lvalue = NUM2LONG(obj);
					}
					if (lvalue < SHRT_MIN || lvalue > SHRT_MAX) {
						rb_raise(rb_eRangeError, "short integer overflow");
					}
					*(short *)var->sqldata = lvalue;
					offset += alignment;
					break;

				case SQL_LONG :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--)
							ratio *= 10;
						obj = double_from_obj(obj);
						dvalue = NUM2DBL(obj) * ratio;
						lvalue = (ISC_LONG)(dvalue + 0.5);
					} else {
						obj = long_from_obj(obj);
						lvalue = NUM2LONG(obj);
					}
					if (lvalue < -2147483647 || lvalue > 2147483647) {
                        rb_raise(rb_eRangeError, "integer overflow");
					}
					*(ISC_LONG *)var->sqldata = (ISC_LONG)lvalue;
					offset += alignment;
					break;

				case SQL_FLOAT :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = double_from_obj(obj);
					dvalue = NUM2DBL(obj);
					if (dvalue >= 0.0) {
						dcheck = dvalue;
					} else {
						dcheck = dvalue * -1;
					}
					if (dcheck != 0.0 && (dcheck < FLT_MIN || dcheck > FLT_MAX)) {
						rb_raise(rb_eRangeError, "float overflow");
					}
					*(float *)var->sqldata = (float)dvalue;
					offset += alignment;
					break;

				case SQL_DOUBLE :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = double_from_obj(obj);
					dvalue = NUM2DBL(obj);
					*(double *)var->sqldata = dvalue;
					offset += alignment;
					break;
#if HAVE_LONG_LONG
				case SQL_INT64 :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);

					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--)
							ratio *= 10;
						obj = double_from_obj(obj);
						dvalue = NUM2DBL(obj) * ratio;
						llvalue = (ISC_INT64)(dvalue + 0.5);
					} else {
						obj = ll_from_obj(obj);
						llvalue = NUM2LL(obj);
					}

					*(ISC_INT64 *)var->sqldata = llvalue;
					offset += alignment;
					break;
#endif
				case SQL_BLOB :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = rb_obj_as_string(obj);

					blob_handle = 0;
					isc_create_blob2(
						fb_connection->isc_status,&fb_connection->db,&fb_connection->transact,
						&blob_handle,&blob_id,0,NULL);
					fb_error_check(fb_connection->isc_status);
					length = RSTRING_LEN(obj);
					p = RSTRING_PTR(obj);
					while (length >= 4096) {
						isc_put_segment(fb_connection->isc_status,&blob_handle,4096,p);
						fb_error_check(fb_connection->isc_status);
						p += 4096;
						length -= 4096;
					}
					if (length) {
						isc_put_segment(fb_connection->isc_status,&blob_handle,length,p);
						fb_error_check(fb_connection->isc_status);
					}
					isc_close_blob(fb_connection->isc_status,&blob_handle);
					fb_error_check(fb_connection->isc_status);

					*(ISC_QUAD *)var->sqldata = blob_id;
					offset += alignment;
					break;

				case SQL_TIMESTAMP :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_timestamp(&tms, obj);
					isc_encode_timestamp(&tms, (ISC_TIMESTAMP *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_TIME :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_timestamp(&tms, obj);
					isc_encode_sql_time(&tms, (ISC_TIME *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_DATE :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_date(&tms, obj);
					isc_encode_sql_date(&tms, (ISC_DATE *)var->sqldata);
					offset += alignment;
					break;


				default :
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%d)", dtp);
			}

			if (var->sqltype & 1) {
				offset = FB_ALIGN(offset, sizeof(short));
				var->sqlind = (short *)(fb_cursor->i_buffer + offset);
				*var->sqlind = 0;
				offset += sizeof(short);
			}
		} else if (var->sqltype & 1) {
			var->sqldata = 0;
			offset = FB_ALIGN(offset, sizeof(short));
			var->sqlind = (short *)(fb_cursor->i_buffer + offset);
			*var->sqlind = -1;
			offset += sizeof(short);
		} else {
			rb_raise(rb_eFbError, "specified column is not permitted to be null");
		}
	}
}
*/

func (cursor *Cursor) executeWithParams(args []interface{}) (err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if err = cursor.setInputParams(args); err != nil {
		return
	}
	C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, cursor.i_sqlda, (*C.XSQLDA)(nil))
	return fbErrorCheck(&isc_status)
}

func (cursor *Cursor) fbCursorDrop() (err os.Error) {
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

func (cursor *Cursor) drop() (err os.Error) {
	err = cursor.fbCursorDrop()
	cursor.Fields = nil
	cursor.FieldsMap = nil
	for i, c := range cursor.connection.cursors {
		if c == cursor {
			cursor.connection.cursors[i] = nil
		}
	}
	return
}

func (cursor *Cursor) rowsAffected(statement C.long) int {
	// TODO: implement
	return 0
}

var reLowercase = regexp.MustCompile("[a-z]")

func hasLowercase(s string) bool {
	return reLowercase.MatchString(s)
}

func sqlTypeFromCode(code, subType C.ISC_SHORT) string {
	switch code {
	case C.SQL_TEXT, C.blr_text:
		return "CHAR"
	case C.SQL_VARYING, C.blr_varying:
		return "VARCHAR"
	case C.SQL_SHORT, C.blr_short:
		switch subType {
		case 0:
			return "SMALLINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case C.SQL_LONG, C.blr_long:
		switch subType {
		case 0:
			return "INTEGER"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
		break
	case C.SQL_FLOAT, C.blr_float:
		return "FLOAT"
	case C.SQL_DOUBLE, C.blr_double:
		switch subType {
		case 0:
			return "DOUBLE PRECISION"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case C.SQL_D_FLOAT, C.blr_d_float:
		return "DOUBLE PRECISION"
	case C.SQL_TIMESTAMP, C.blr_timestamp:
		return "TIMESTAMP"
	case C.SQL_BLOB, C.blr_blob:
		return "BLOB"
	case C.SQL_ARRAY:
		return "ARRAY"
	case C.SQL_QUAD, C.blr_quad:
		return "DECIMAL"
	case C.SQL_TYPE_TIME, C.blr_sql_time:
		return "TIME"
	case C.SQL_TYPE_DATE, C.blr_sql_date:
		return "DATE"
	case C.SQL_INT64, C.blr_int64:
		switch subType {
		case 0:
			return "BIGINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	}
	return fmt.Sprintf("UNKNOWN %d, %d", code, subType)
}

func precisionFromSqlvar(sqlvar *C.XSQLVAR) int {
	switch sqlvar.sqltype & ^1 {
	case C.SQL_SHORT:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 4
		case 2:
			return 4
		}
	case C.SQL_LONG:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 9
		case 2:
			return 9
		}
	case C.SQL_DOUBLE, C.SQL_D_FLOAT:
		switch sqlvar.sqlsubtype {
		case 0:
			return -1
		case 1:
			return 15
		case 2:
			return 15
		}
	case C.SQL_INT64:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 18
		case 2:
			return 18
		}
		break
	}
	return -1
}

func fieldsFromSqlda(sqlda *C.XSQLDA, lowercaseNames bool) []*Field {
	cols := sqlda.sqld
	if cols == 0 {
		return nil
	}

	ary := make([]*Field, cols)
	for count := C.ISC_SHORT(0); count < cols; count++ {
		var field Field

		sqlvar := C.sqlda_sqlvar(sqlda, count)
		dtp := sqlvar.sqltype & ^1

		if sqlvar.aliasname_length > 0 {
			field.Name = C.GoStringN((*C.char)(unsafe.Pointer(&sqlvar.aliasname[0])), C.int(sqlvar.aliasname_length))
		} else {
			field.Name = C.GoStringN((*C.char)(unsafe.Pointer(&sqlvar.sqlname[0])), C.int(sqlvar.sqlname_length))
		}
		if lowercaseNames && !hasLowercase(field.Name) {
			field.Name = strings.ToLower(field.Name)
		}
		field.TypeCode = int(sqlvar.sqltype & ^1)
		field.SqlType = sqlTypeFromCode(dtp, sqlvar.sqlsubtype)
		field.SqlSubtype = int(sqlvar.sqlsubtype)
		field.DisplaySize = int(sqlvar.sqllen)
		if dtp == C.SQL_VARYING {
			field.InternalSize = int(sqlvar.sqllen + C.SHORT_SIZE)
		} else {
			field.InternalSize = int(sqlvar.sqllen)
		}
		field.Precision = precisionFromSqlvar(sqlvar)
		field.Scale = int(sqlvar.sqlscale)
		field.Nullable = (sqlvar.sqltype & 1) != 0

		ary[count] = &field
	}
	return ary
}

func fieldsMapFromSlice(fields []*Field) map[string]*Field {
	m := make(map[string]*Field, len(fields))
	for _, f := range fields {
		m[f.Name] = f
	}
	return m
}

func (cursor *Cursor) check() os.Error {
	if cursor.stmt == 0 {
		return &Error{Message: "dropped cursor"}
	}
	if !cursor.open {
		return &Error{Message: "closed cursor"}
	}
	return nil
}

func (cursor *Cursor) Close() (err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if err = cursor.check(); err != nil {
		return
	}
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
	cursor.Fields = nil
	cursor.FieldsMap = nil
	return
}

func fbAlign(n C.ISC_SHORT, b C.ISC_SHORT) C.ISC_SHORT {
	return (n + b - 1) & ^(b - 1)
}

func (cursor *Cursor) prep() (err os.Error) {
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

func (cursor *Cursor) Fetch(row interface{}) (err os.Error) {
	const SQLCODE_NOMORE = 100
	var isc_status [20]C.ISC_STATUS

	if err = cursor.prep(); err != nil {
		return
	}
	if err = cursor.connection.check(); err != nil {
		return
	}
	if cursor.eof {
		err = &Error{Message: "Cursor is past end of data."}
		return
	}
	// fetch one row 
	if C.isc_dsql_fetch(&isc_status[0], &cursor.stmt, C.SQLDA_VERSION1, cursor.o_sqlda) == SQLCODE_NOMORE {
		cursor.eof = true
		err = os.EOF
		return
	}
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// create result tuple
	cols := cursor.o_sqlda.sqld
	ary := make([]interface{}, cols)
	// set result value for each column
	for count := C.ISC_SHORT(0); count < cols; count++ {
		var val interface{}
		// sqlvar := &cursor.o_sqlda.sqlvar[count]
		sqlvar := C.sqlda_sqlvar(cursor.o_sqlda, count)
		dtp := sqlvar.sqltype & ^1

		// check if column is null
		if (sqlvar.sqltype&1 != 0) && (*sqlvar.sqlind < 0) {
			val = nil
		} else {
			// set column value to result tuple
			switch dtp {
			case C.SQL_TEXT:
				val = C.GoStringN((*C.char)(unsafe.Pointer(sqlvar.sqldata)), C.int(sqlvar.sqllen))
			case C.SQL_SHORT:
				sval := *(*C.short)(unsafe.Pointer(sqlvar.sqldata))
				if sqlvar.sqlscale < 0 {
					ratio := C.short(1)
					for scnt := C.ISC_SHORT(0); scnt > sqlvar.sqlscale; scnt-- {
						ratio *= 10
					}
					dval := float64(sval) / float64(ratio)
					val = dval
				} else {
					val = int16(sval)
				}
			case C.SQL_LONG:
				lval := *(*C.ISC_LONG)(unsafe.Pointer(sqlvar.sqldata))
				if sqlvar.sqlscale < 0 {
					ratio := C.short(1)
					for scnt := C.ISC_SHORT(0); scnt > sqlvar.sqlscale; scnt-- {
						ratio *= 10
					}
					dval := float64(lval) / float64(ratio)
					val = dval
				} else {
					val = int32(lval)
				}
			}
		}
		ary[count] = val
	}
	switch row := row.(type) {
	case *[]interface{}:
		*row = ary
	default:
		err = os.NewError(fmt.Sprintf("Unsupported row type: %T", row))
	}
	return
}
/*
static VALUE fb_cursor_fetch(struct FbCursor *fb_cursor)
{
	struct FbConnection *fb_connection;
	long cols;
	VALUE ary;
	long count;
	XSQLVAR *var;
	long dtp;
	VALUE val;
	VARY *vary;
	double ratio;
	double dval;
	long scnt;
	struct tm tms;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	unsigned short actual_seg_len;
	static char blob_items[] = {
		isc_info_blob_max_segment,
		isc_info_blob_num_segments,
		isc_info_blob_total_length
	};
	char blob_info[32];
	char *p, item;
	short length;
	unsigned short max_segment = 0;
	ISC_LONG num_segments = 0;
	ISC_LONG total_length = 0;

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	if (fb_cursor->eof) {
		rb_raise(rb_eFbError, "Cursor is past end of data.");
	}
	 // Fetch one row 
	if (isc_dsql_fetch(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda) == SQLCODE_NOMORE) {
		fb_cursor->eof = Qtrue;
		return Qnil;
	}
	fb_error_check(fb_connection->isc_status);

	 // Create the result tuple object 
	cols = fb_cursor->o_sqlda->sqld;
	ary = rb_ary_new2(cols);

	 // Create the result objects for each columns 
	for (count = 0; count < cols; count++) {
		var = &fb_cursor->o_sqlda->sqlvar[count];
		dtp = var->sqltype & ~1;

		 // Check if column is null 

		if ((var->sqltype & 1) && (*var->sqlind < 0)) {
			val = Qnil;
		} else {
			 // Set the column value to the result tuple 

			switch (dtp) {
				case SQL_TEXT:
					val = rb_tainted_str_new(var->sqldata, var->sqllen);
					break;

				case SQL_VARYING:
					vary = (VARY*)var->sqldata;
					val = rb_tainted_str_new(vary->vary_string, vary->vary_length);
					break;

				case SQL_SHORT:
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--) ratio *= 10;
						dval = (double)*(short*)var->sqldata/ratio;
						val = rb_float_new(dval);
					} else {
						val = INT2NUM((long)*(short*)var->sqldata);
					}
					break;

				case SQL_LONG:
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--) ratio *= 10;
						dval = (double)*(ISC_LONG*)var->sqldata/ratio;
						val = rb_float_new(dval);
					} else {
						val = INT2NUM(*(ISC_LONG*)var->sqldata);
					}
					break;

				case SQL_FLOAT:
					val = rb_float_new((double)*(float*)var->sqldata);
					break;

				case SQL_DOUBLE:
					val = rb_float_new(*(double*)var->sqldata);
					break;
#if HAVE_LONG_LONG
				case SQL_INT64:
        				if (var->sqlscale < 0) {
        					ratio = 1;
        					for (scnt = 0; scnt > var->sqlscale; scnt--) ratio *= 10;
        					dval = (double)*(long*)var->sqldata/ratio;
        					val = rb_float_new(dval);
        				} else {
        					val = LL2NUM(*(LONG_LONG*)var->sqldata);
        				}
					break;
#endif
				case SQL_TIMESTAMP:
					isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &tms);
					val = fb_mktime(&tms, "local");
					break;

				case SQL_TYPE_TIME:
					isc_decode_sql_time((ISC_TIME *)var->sqldata, &tms);
					tms.tm_year = 100;
					tms.tm_mon = 0;
					tms.tm_mday = 1;
					val = fb_mktime(&tms, "utc");
					break;

				case SQL_TYPE_DATE:
					isc_decode_sql_date((ISC_DATE *)var->sqldata, &tms);
					val = fb_mkdate(&tms);
					break;

				case SQL_BLOB:
					blob_handle = 0;
					blob_id = *(ISC_QUAD *)var->sqldata;
					isc_open_blob2(fb_connection->isc_status, &fb_connection->db, &fb_connection->transact, &blob_handle, &blob_id, 0, NULL);
					fb_error_check(fb_connection->isc_status);
					isc_blob_info(
						fb_connection->isc_status, &blob_handle,
						sizeof(blob_items), blob_items,
						sizeof(blob_info), blob_info);
					fb_error_check(fb_connection->isc_status);
					for (p = blob_info; *p != isc_info_end; p += length) {
						item = *p++;
						length = (short) isc_vax_integer(p,2);
						p += 2;
						switch (item) {
							case isc_info_blob_max_segment:
								max_segment = isc_vax_integer(p,length);
								break;
							case isc_info_blob_num_segments:
								num_segments = isc_vax_integer(p,length);
								break;
							case isc_info_blob_total_length:
								total_length = isc_vax_integer(p,length);
								break;
						}
					}
					val = rb_tainted_str_new(NULL,total_length);
					for (p = RSTRING_PTR(val); num_segments > 0; num_segments--, p += actual_seg_len) {
						isc_get_segment(fb_connection->isc_status, &blob_handle, &actual_seg_len, max_segment, p);
						fb_error_check(fb_connection->isc_status);
					}
					isc_close_blob(fb_connection->isc_status, &blob_handle);
					fb_error_check(fb_connection->isc_status);
					break;

				case SQL_ARRAY:
					rb_warn("ARRAY not supported (yet)");
					val = Qnil;
					break;

				default:
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%ld)", dtp);
					break;
			}
		}
		rb_ary_push(ary, val);
	}

	return ary;
}
*/
