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
	sqlda->version = SQLDA_VERSION1;
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

XSQLVAR* sqlda_sqlvar(XSQLDA* sqlda, ISC_SHORT col) {
	return sqlda->sqlvar + col;
}

#define SHORT_SIZE sizeof(short)
*/
import "C"

import (
	"os"
	"unsafe"
	"fmt"
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
		fmt.Println("here 1")
		return
	}
	return cursor, nil
}

func (cursor *Cursor) fbCursorDrop() (err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if cursor.open {
		C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
		if err = fbErrorCheck(&isc_status); err != nil {
			fmt.Println("here 2")
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
	const dialect = 1
	var isc_status [20]C.ISC_STATUS

	// prepare query
	sql2 := C.CString(sql)
	defer C.free(unsafe.Pointer(sql2))
	sql3 := (*C.ISC_SCHAR)(unsafe.Pointer(sql2))
	C.isc_dsql_prepare(&isc_status[0], &cursor.connection.transact, &cursor.stmt, 0, sql3, cursor.connection.dialect, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 3")
		return
	}
	// get statement type
	isc_info_stmt := [...]C.ISC_SCHAR{C.isc_info_sql_stmt_type}
	var isc_info_buff [16]C.ISC_SCHAR
	C.isc_dsql_sql_info(&isc_status[0], &cursor.stmt,
		C.short(unsafe.Sizeof(isc_info_stmt[0])), &isc_info_stmt[0],
		C.short(unsafe.Sizeof(isc_info_buff[0]) * 16), &isc_info_buff[0])
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 4")
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
	C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, dialect, cursor.i_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 5")
		return
	}
	// describe output parameters
	C.isc_dsql_describe(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 6")
		return
	}
	// get number of parameters and reallocate SQLDA
	in_params := cursor.i_sqlda.sqld
	if cursor.i_sqlda.sqln < in_params {
		C.free(unsafe.Pointer(cursor.i_sqlda))
		cursor.i_sqlda = C.sqlda_alloc(C.long(in_params))
		// describe again 
		C.isc_dsql_describe_bind(&isc_status[0], &cursor.stmt, dialect, cursor.i_sqlda)
		if err = fbErrorCheck(&isc_status); err != nil {
			fmt.Println("here 7")
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
		fmt.Println("path A")
		// open cursor if statement is query 
		// get number of columns and reallocate SQLDA 
		cols := cursor.o_sqlda.sqld
		if cursor.o_sqlda.sqln < cols {
			C.free(unsafe.Pointer(cursor.o_sqlda))
			cursor.o_sqlda = C.sqlda_alloc(C.long(cols))
			// describe again 
			C.isc_dsql_describe(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda)
			if err = fbErrorCheck(&isc_status); err != nil {
				fmt.Println("here 8")
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
			fmt.Println("here 9")
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
		fmt.Println("path B")
		// execute statement if not query
		if statement == C.isc_info_sql_stmt_start_trans {
			panic("use fb.Connection.Transaction()")
		} else if statement == C.isc_info_sql_stmt_commit {
			panic("use fb.Connection.Commit()")
		} else if statement == C.isc_info_sql_stmt_rollback {
			panic("use fb.Connection.Rollback()")
		} else if in_params > 0 {
			fmt.Println("path B1")
			cursor.executeWithParams(args)
		} else {
			fmt.Println("path B2")
			C.isc_dsql_execute2(&isc_status[0], &cursor.connection.transact, &cursor.stmt, C.SQLDA_VERSION1, (*C.XSQLDA)(nil), (*C.XSQLDA)(nil))
			if err = fbErrorCheck(&isc_status); err != nil {
				fmt.Println("here 10")
				return
			}
		}
		rowsAffected = cursor.rowsAffected(statement)
	}
	fmt.Println("here 11")
	return
}

func (cursor *Cursor) execute(sql string, args ...interface{}) (rowsAffected int, err os.Error) {
	var isc_status [20]C.ISC_STATUS

	if cursor.open {
		C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
		if err = fbErrorCheck(&isc_status); err != nil {
			fmt.Println("here 12")
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
	fmt.Println("here 13")
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
		fmt.Println("here 14")
		return
	}
	C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_close)
	if err = fbErrorCheckWarn(&isc_status); err != nil {
		fmt.Println("here 15")
		return
	}
	C.isc_dsql_free_statement(&isc_status[0], &cursor.stmt, C.DSQL_drop)
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 16")
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
	C.isc_dsql_describe(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda)
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here prep")
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
/*
static void fb_cursor_fetch_prep(struct FbCursor *fb_cursor)
{
	struct FbConnection *fb_connection;
	long cols;
	long count;
	XSQLVAR *var;
	short dtp;
	long length;
	long alignment;
	long offset;

	fb_cursor_check(fb_cursor);

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	 // Check if open cursor 
	if (!fb_cursor->open) {
		rb_raise(rb_eFbError, "The cursor has not been opened. Use execute(query)");
	}
	 // Describe output SQLDA 
	isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda);
	fb_error_check(fb_connection->isc_status);

	 // Set the output SQLDA 
	cols = fb_cursor->o_sqlda->sqld;
	for (var = fb_cursor->o_sqlda->sqlvar, offset = 0, count = 0; count < cols; var++, count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT) {
			alignment = 1;
		} else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}
		offset = FB_ALIGN(offset, alignment);
		var->sqldata = (char*)(fb_cursor->o_buffer + offset);
		offset += length;
		offset = FB_ALIGN(offset, sizeof(short));
		var->sqlind = (short*)(fb_cursor->o_buffer + offset);
		offset += sizeof(short);
	}
}
*/
func (cursor *Cursor) Fetch(row interface{}) (err os.Error) {
	const SQLCODE_NOMORE = 100
	var isc_status [20]C.ISC_STATUS

	fmt.Println("in Fetch 1")
	if err = cursor.prep(); err != nil {
		fmt.Println("here after prep")
		return
	}
	fmt.Println("in Fetch 2")
	if err = cursor.connection.check(); err != nil {
		fmt.Println("here 17")
		return
	}
	fmt.Println("in Fetch 3")
	if cursor.eof {
		err = &Error{Message: "Cursor is past end of data."}
		fmt.Println("here 18")
		return
	}
	fmt.Println("in Fetch 4")
	// fetch one row 
	if (C.isc_dsql_fetch(&isc_status[0], &cursor.stmt, 1, cursor.o_sqlda) == SQLCODE_NOMORE) {
		cursor.eof = true
		err = os.EOF
		fmt.Println("here 19")
		return
	}
	fmt.Println("in Fetch 5")
	if err = fbErrorCheck(&isc_status); err != nil {
		fmt.Println("here 20")
		return
	}
	fmt.Println("in Fetch 6")
	// create result tuple
	cols := cursor.o_sqlda.sqld
	ary := make([]interface{}, cols)
	// set result value for each column
	for count := C.ISC_SHORT(0); count < cols; count++ {
		fmt.Println("in Fetch 7")
		var val interface{}
		fmt.Println("in Fetch 8")
		// sqlvar := &cursor.o_sqlda.sqlvar[count]
		sqlvar := C.sqlda_sqlvar(cursor.o_sqlda, count)
		fmt.Println("in Fetch 9")
		dtp := sqlvar.sqltype & ^1;

		// check if column is null
		if ((sqlvar.sqltype & 1 != 0) && (*sqlvar.sqlind < 0)) {
			val = nil
		} else {
			// set column value to result tuple
			switch dtp {
			case C.SQL_TEXT:
				val = C.GoStringN((*C.char)(unsafe.Pointer(sqlvar.sqldata)), C.int(sqlvar.sqllen))
			}
		}
		ary[count] = val
	}
	switch row := row.(type) {
	case *[]interface{}:
		*row = ary
	}
	fmt.Println("here 21")
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
