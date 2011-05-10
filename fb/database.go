package fb

import (
	"os"
	"strings"
	"strconv"
)

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
