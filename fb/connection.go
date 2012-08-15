package fb

/*
#cgo darwin CFLAGS: -D_XOPEN_SOURCE -D_DARWIN_C_SOURCE -I/Library/Frameworks/Firebird.framework/Headers
#cgo darwin LDFLAGS: -L. -arch x86_64 -framework Firebird
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

import "unsafe"

type Connection struct {
	database     *Database
	db           C.isc_db_handle
	transact     C.isc_tr_handle
	dialect      C.ushort
	db_dialect   C.ushort
	dropped      bool
	cursors      []*Cursor
	rowsAffected int
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
		if err = fbErrorCheck(&isc_status); err != nil {
			return
		}
	}
	return nil
}

func (conn *Connection) transactionStart(options *string) error {
	var isc_status [20]C.ISC_STATUS

	if conn.TransactionStarted() {
		return &Error{Message: "A transaction has been already started"}
	}
	var tpb *C.char = (*C.char)(nil)
	var tpb_len C.long = 0
	if options != nil {
		options2 := C.CString(*options)
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

func (conn *Connection) Rollback() {

}
