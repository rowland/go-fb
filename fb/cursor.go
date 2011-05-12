package fb

/*
#include <ibase.h>
#include <stdlib.h>
*/
import "C"

type Cursor struct {
	open int
	eof int
	auto_transact C.isc_tr_handle
	stmt C.isc_stmt_handle
	i_sqlda *C.XSQLDA
	o_sqlda *C.XSQLDA
	i_buffer *C.char
	i_buffer_size C.long
	o_buffer *C.char
	o_buffer_size C.long
	// VALUE fields_ary
	// VALUE fields_hash;
	// VALUE connection;
}

func (cursor *Cursor) Drop() {
	
}