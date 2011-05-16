package fb

/*
#include <ibase.h>
#include <stdlib.h>

#define	SQLDA_COLSINIT	50
#define	FB_ALIGN(n, b)	((n + b - 1) & ~(b - 1))

static XSQLDA* sqlda_alloc(long cols)
{
	XSQLDA *sqlda;

	sqlda = (XSQLDA*)malloc(XSQLDA_LENGTH(cols));
#ifdef SQLDA_CURRENT_VERSION
	sqlda->version = SQLDA_CURRENT_VERSION;
#else
	sqlda->version = SQLDA_VERSION1;
#endif
	sqlda->sqln = cols;
	sqlda->sqld = 0;
	return sqlda;
}

static long calculate_buffsize(XSQLDA *sqlda)
{
	XSQLVAR *var;
	long cols;
	short dtp;
	long offset = 0;
	long alignment;
	long length;
	long count;

	cols = sqlda->sqld;
	var = sqlda->sqlvar;
	for (count = 0; count < cols; var++,count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT) {
			alignment = 1;
		} else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}

		offset = FB_ALIGN(offset, alignment);
		offset += length;
		offset = FB_ALIGN(offset, sizeof(short));
		offset += sizeof(short);
	}

	return offset + sizeof(short);
}
*/
import "C"

import (
	"os"
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
	// VALUE fields_ary
	// VALUE fields_hash;
	// VALUE connection;
}

const SQLDA_COLSINIT = 50

func newCursor(conn *Connection) (cursor *Cursor, err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if err = conn.check(); err != nil {
		return
	}
	cursor = &Cursor{connection: conn}
	cursor.i_sqlda = C.sqlda_alloc(SQLDA_COLSINIT)
	cursor.o_sqlda = C.sqlda_alloc(SQLDA_COLSINIT)
	C.isc_dsql_alloc_statement2(&isc_status[0], &conn.db, &cursor.stmt)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	return cursor, nil
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
	// fb_cursor->fields_ary = Qnil;
	// fb_cursor->fields_hash = Qnil;
	for i, c := range cursor.connection.cursors {
		if c == cursor {
			cursor.connection.cursors[i] = nil
		}
	}
	return
}

func (cursor *Cursor) setInputParams(args []interface{}) {

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

func (cursor *Cursor) executeWithParams(args []interface{}) {

}

func (cursor *Cursor) rowsAffected(statement C.long) int {
	return 0
}

func (cursor *Cursor) execute2(sql string, args ...interface{}) (rowsAffected int, err os.Error) {
	var isc_status [20]C.ISC_STATUS

	// prepare query
	sql2 := C.CString(sql)
	defer C.free(unsafe.Pointer(sql2))
	sql3 := (*C.ISC_SCHAR)(unsafe.Pointer(sql2))
	C.isc_dsql_prepare(&isc_status[0], &cursor.connection.transact, &cursor.stmt, 0, sql3, cursor.connection.dialect, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// get statement type
	isc_info_stmt := [...]C.ISC_SCHAR{C.isc_info_sql_stmt_type}
	var isc_info_buff [16]C.ISC_SCHAR
	C.isc_dsql_sql_info(&isc_status[0], &cursor.stmt,
		C.short(unsafe.Sizeof(isc_info_stmt)), &isc_info_stmt[0],
		C.short(unsafe.Sizeof(isc_info_buff)), &isc_info_buff[0])
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
	C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, 1, cursor.i_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// describe output parameters
	C.isc_dsql_describe(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		return
	}
	// get number of parameters and reallocate SQLDA
	in_params := cursor.i_sqlda.sqld
	if cursor.i_sqlda.sqln < in_params {
		C.free(unsafe.Pointer(cursor.i_sqlda))
		cursor.i_sqlda = C.sqlda_alloc(C.long(in_params))
		// describe again 
		C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, 1, cursor.i_sqlda)
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
			C.isc_dsql_describe(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda)
			if err = fbErrorCheck(&isc_status); err != nil {
				return
			}
		}

		var i_sqlda *C.XSQLDA
		if in_params > 0 {
			cursor.setInputParams(args)
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
		// cursor.fields_ary = fb_cursor_fields_ary(cursor.o_sqlda, fb_connection.downcase_names);
		// cursor.fields_hash = fb_cursor_fields_hash(cursor.fields_ary);
	} else {
		// execute statement if not query
		if statement == C.isc_info_sql_stmt_start_trans {
			panic("use fb.Connection.Ttransaction()")
		} else if statement == C.isc_info_sql_stmt_commit {
			panic("use fb.Connection.Commit()")
		} else if statement == C.isc_info_sql_stmt_rollback {
			panic("use fb.Connection.Rollback()")
		} else if in_params > 0 {
			cursor.executeWithParams(args)
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

func (cursor *Cursor) check() os.Error {
	if cursor.stmt == 0 {
		return &Error{Message: "dropped cursor"}
	}
	if !cursor.open {
		return &Error{Message: "closed cursor"}
	}
	return nil
}

func (cursor *Cursor) close() (err os.Error) {
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
	// fb_cursor->fields_ary = Qnil;
	// fb_cursor->fields_hash = Qnil;
	return
}
