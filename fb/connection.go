package fb

/*
#include <ibase.h>
#include <stdlib.h>
*/
import "C"

import (
	"os"
)

type Connection struct {
	database   *Database
	db         C.isc_db_handle
	transact   C.isc_tr_handle
	dialect    C.ushort
	db_dialect C.ushort
	dropped    bool
	isc_status [20]C.ISC_STATUS
	cursors    []*Cursor
}

func (conn *Connection) check() os.Error {
	if conn.db == 0 {
		return &Error{0, "closed db connection"}
	}
	return nil
}

func (conn *Connection) disconnect() (err os.Error) {
	if conn.transact != 0 {
		C.isc_commit_transaction(&conn.isc_status[0], &conn.transact)
		if err = fbErrorCheck(&conn.isc_status); err != nil {
			return
		}
	}
	if conn.dropped {
		C.isc_drop_database(&conn.isc_status[0], &conn.db)
	} else {
		C.isc_detach_database(&conn.isc_status[0], &conn.db)
	}
	return fbErrorCheck(&conn.isc_status)
}

func (conn *Connection) dropCursors() {
	for _, cursor := range conn.cursors {
		cursor.Drop()
	}
	conn.cursors = nil
}

func (conn *Connection) Close() (err os.Error) {
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

func (conn *Connection) Drop() (err os.Error) {
	conn.dropped = true
	if err = conn.disconnect(); err != nil {
		return
	}
	conn.dropCursors()
	return nil
}
