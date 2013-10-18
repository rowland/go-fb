package fb

/*
#include <ibase.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"time"
	"unsafe"
)

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Error struct {
	Code    int
	Message string
}

func (this Error) Error() string {
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
	TimeZone       string
}

func MapFromConnectionString(parms string) (map[string]string, error) {
	m := make(map[string]string)
	kva := strings.Split(parms, ";")
	for _, kv := range kva {
		pair := strings.SplitN(kv, "=", 2)
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

func New(parms string) (db *Database, err error) {
	p, err := MapFromConnectionString(parms)
	database, ok := p["database"]
	if !ok {
		return nil, errors.New("database parm required")
	}
	username, ok := p["username"]
	if !ok {
		return nil, errors.New("username parm required")
	}
	password, ok := p["password"]
	if !ok {
		return nil, errors.New("password parm required")
	}
	charset, _ := p["charset"]
	role, _ := p["role"]
	lowercaseNames := false
	sLowercaseNames, ok := p["lowercase_names"]
	if ok {
		lowercaseNames, _ = strconv.ParseBool(sLowercaseNames)
	}
	pageSize := 1024
	sPageSize, ok := p["page_size"]
	if ok {
		pageSize, err = strconv.Atoi(sPageSize)
		if err != nil {
			return nil, errors.New("Invalid page_size: " + err.Error())
		}
	}
	timezone, _ := p["timezone"]
	db = &Database{database, username, password, role, charset, lowercaseNames, pageSize, timezone}
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

func (db *Database) Create() (*Connection, error) {
	var isc_status [20]C.ISC_STATUS
	var handle C.isc_db_handle = 0
	var local_transact C.isc_tr_handle = 0
	sql := C.CString(db.CreateStatement())
	sql2 := (*C.ISC_SCHAR)(unsafe.Pointer(sql))
	defer C.free(unsafe.Pointer(sql))

	if C.isc_dsql_execute_immediate(&isc_status[0], &handle, &local_transact, 0, sql2, 3, nil) != 0 {
		return nil, fbErrorCheck(&isc_status)
	}
	location, err := time.LoadLocation(db.TimeZone)
	if err != nil {
		location = time.Local
	}
	return &Connection{database: db, db: handle, Location: location}, nil
}

func Create(parms string) (conn *Connection, err error) {
	db, err := New(parms)
	if err != nil {
		return
	}
	conn, err = db.Create()
	return
}

func (db *Database) createDbp() string {
	var buf bytes.Buffer
	buf.WriteByte(C.isc_dpb_version1)

	buf.WriteByte(C.isc_dpb_user_name)
	buf.WriteByte(byte(len(db.Username)))
	buf.WriteString(db.Username)

	buf.WriteByte(C.isc_dpb_password)
	buf.WriteByte(byte(len(db.Password)))
	buf.WriteString(db.Password)

	if db.Charset != "" {
		buf.WriteByte(C.isc_dpb_lc_ctype)
		buf.WriteByte(byte(len(db.Charset)))
		buf.WriteString(db.Charset)
	}

	if db.Role != "" {
		buf.WriteByte(C.isc_dpb_sql_role_name)
		buf.WriteByte(byte(len(db.Role)))
		buf.WriteString(db.Role)
	}

	return buf.String()
}

func (db *Database) Connect() (*Connection, error) {
	var isc_status [20]C.ISC_STATUS
	var handle C.isc_db_handle = 0

	database := C.CString(db.Database)
	database2 := (*C.ISC_SCHAR)(unsafe.Pointer(database))
	defer C.free(unsafe.Pointer(database))

	dbp := db.createDbp()
	dbp2 := C.CString(dbp)
	dbp3 := (*C.ISC_SCHAR)(unsafe.Pointer(dbp2))
	defer C.free(unsafe.Pointer(dbp2))

	var length C.short = C.short(len(dbp))
	C.isc_attach_database(&isc_status[0], 0, database2, &handle, length, dbp3)
	if err := fbErrorCheck(&isc_status); err != nil {
		return nil, err
	}
	location, err := time.LoadLocation(db.TimeZone)
	if err != nil {
		location = time.Local
	}
	return &Connection{database: db, db: handle, Location: location}, nil
}

func Connect(parms string) (conn *Connection, err error) {
	db, err := New(parms)
	if err != nil {
		return
	}
	conn, err = db.Connect()
	return
}

func (db *Database) Drop() (err error) {
	conn, err := db.Connect()
	if err != nil {
		return
	}
	err = conn.Drop()
	return
}

func Drop(parms string) (err error) {
	db, err := New(parms)
	if err != nil {
		return
	}
	return db.Drop()
}
