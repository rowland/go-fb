package fb

/*
#include <ibase.h>
#include <stdlib.h>
*/
import "C"

import (
	"os"
)

type Cursor struct {
	connection    *Connection
	open          bool
	eof           int
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

func (cursor *Cursor) Drop() {
	cursor.fbCursorDrop()
	// fb_cursor->fields_ary = Qnil;
	// fb_cursor->fields_hash = Qnil;
	for i, c := range cursor.connection.cursors {
		if c == cursor {
			cursor.connection.cursors[i] = nil
		}
	}
}
