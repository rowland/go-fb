package fb

import (
	"testing"
)

const (
	TestConnectionString = "database=localhost:/var/fbdata/go-fb-test.fdb; username=rubytest; password=rubytest; charset=NONE; role=READER;"
	TestConnectionString2 = "database=localhost:/var/fbdata/go-fb-test.fdb;username=rubytest;password=rubytest;lowercase_names=true;page_size=2048"
)

func TestMapFromConnectionString(t *testing.T) {
	m, err := MapFromConnectionString(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error parsing connection string: %s", err)
	}
	if m["database"] != "localhost:/var/fbdata/go-fb-test.fdb" {
		t.Error("Error finding key: database")
	}
	if m["username"] != "rubytest" {
		t.Error("Error finding key: database")
	}
	if m["password"] != "rubytest" {
		t.Error("Error finding key: password")
	}
	if m["charset"] != "NONE" {
		t.Error("Error finding key: charset")
	}
	if m["role"] != "READER" {
		t.Error("Error finding key: role")
	}
}

func TestNew(t *testing.T) {
	db, err := New(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}
	if db.Database != "localhost:/var/fbdata/go-fb-test.fdb" {
		t.Error("Database incorrect")
	}
	if db.Username != "rubytest" {
		t.Error("Username incorrect")
	}
	if db.Password != "rubytest" {
		t.Error("Password incorrect")
	}
	if db.Charset != "NONE" {
		t.Error("Charset incorrect")
	}
	if db.Role != "READER" {
		t.Error("Role incorrect")
	}
}

func TestNew2(t *testing.T) {
	db, err := New(TestConnectionString2)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if db.Charset != "" {
		t.Errorf("Charset: unexpected value %s", db.Charset)
	}
	if db.Role != "" {
		t.Errorf("Role: unexpected value %s", db.Role)
	}
	if db.LowercaseNames != true {
		t.Errorf("LowercaseNames: unexpected value %v", db.LowercaseNames)
	}
	if db.PageSize != 2048 {
		t.Errorf("PageSize: unexpected value %d", db.PageSize)
	}
}
