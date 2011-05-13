package fb

/*
#include <ibase.h>
#include <stdlib.h>
*/
import "C"
import "unsafe"

import (
	"os"
	"strings"
	"strconv"
	"fmt"
	"bytes"
)

type Error struct {
	Code int
	Message string
}

func (this Error) String() string {
	return this.Message
}

type Database struct {
	Database       string
	Username       string
	Password       string
	Role           string
	Charset        string
	LowercaseNames bool
	PageSize       int
}

func MapFromConnectionString(parms string) (map[string]string, os.Error) {
	m := make(map[string]string)
	kva := strings.Split(parms, ";", -1)
	for _, kv := range kva {
		pair := strings.Split(kv, "=", 2)
		if len(pair) != 2 {
			continue
		}
		k, v := strings.TrimSpace(pair[0]), strings.TrimSpace(pair[1])
		if k != "" && v != "" {
			m[k] = v
		}
	}
	return m, nil
}

func New(parms string) (db *Database, err os.Error) {
	p, err := MapFromConnectionString(parms)
	database, ok := p["database"]
	if !ok {
		return nil, os.ErrorString("database parm required")
	}
	username, ok := p["username"]
	if !ok {
		return nil, os.ErrorString("username parm required")
	}
	password, ok := p["password"]
	if !ok {
		return nil, os.ErrorString("password parm required")
	}
	charset, _ := p["charset"]
	role, _ := p["role"]
	lowercaseNames := false
	sLowercaseNames, ok := p["lowercase_names"]
	if ok {
		lowercaseNames, _ = strconv.Atob(sLowercaseNames)
	}
	pageSize := 1024
	sPageSize, ok := p["page_size"]
	if ok {
		pageSize, err = strconv.Atoi(sPageSize)
		if err != nil {
			return nil, os.NewError("Invalid page_size: " + err.String())
		}
	}
	db = &Database{database, username, password, role, charset, lowercaseNames, pageSize}
	return db, nil
}

func (db *Database) CreateStatement() string {
	var defaultCharset string
	if db.Charset != "" {
		defaultCharset = fmt.Sprintf("DEFAULT CHARACTER SET %s", db.Charset)
	}
	return fmt.Sprintf("CREATE DATABASE '%s' USER '%s' PASSWORD '%s' PAGE_SIZE = %d %s;",
		db.Database, db.Username, db.Password, db.PageSize, defaultCharset)
}

func fbErrorMsg(isc_status *C.ISC_STATUS) string {
	var msg [1024]C.ISC_SCHAR
	var buf bytes.Buffer
	for C.fb_interpret(&msg[0], 1024, &isc_status) != 0 {
		for i:= 0; msg[i] != 0; i++ {
			buf.WriteByte(uint8(msg[i]))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func fbErrorCheck(isc_status *[20]C.ISC_STATUS) *Error {
	if (isc_status[0] == 1 && isc_status[1] != 0) {
		var msg [1024]C.ISC_SCHAR
		var code C.short = C.short(C.isc_sqlcode(&isc_status[0]))

		C.isc_sql_interprete(code, &msg[0], 1024);
		var buf bytes.Buffer
		for i:= 0; msg[i] != 0; i++ {
			buf.WriteByte(uint8(msg[i]))
		}
		buf.WriteString("\n")
		buf.WriteString(fbErrorMsg(&isc_status[0]))

		return &Error{int(code), buf.String()}
	}
	return nil
}

func (db *Database) Create() (*Connection, os.Error) {
	var isc_status [20]C.ISC_STATUS
	var handle C.isc_db_handle = 0
	var local_transact C.isc_tr_handle = 0
	sql := C.CString(db.CreateStatement())
	sql2 := (*C.ISC_SCHAR)(unsafe.Pointer(sql));
	defer C.free(unsafe.Pointer(sql))
	
	if C.isc_dsql_execute_immediate(&isc_status[0], &handle, &local_transact, 0, sql2, 3, nil) != 0 {
		return nil, fbErrorCheck(&isc_status)
	}
	return &Connection{database: db, db: handle}, nil
}

func Create(parms string) (conn *Connection, err os.Error) {
	db, err := New(parms)
	if err != nil {
		return
	}
	conn, err = db.Create()
	return
}
