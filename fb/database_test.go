package fb

import (
	"testing"
	"os"
	"bufio"
	"fmt"
)

const (
	TestFilename = "/var/fbdata/go-fb-test.fdb"
	TestConnectionString = "database=localhost:/var/fbdata/go-fb-test.fdb; username=gotest; password=gotest; charset=NONE; role=READER;"
	TestConnectionString2 = "database=localhost:/var/fbdata/go-fb-test.fdb;username=gotest;password=gotest;lowercase_names=true;page_size=2048"
	CreateStatement = "CREATE DATABASE 'localhost:/var/fbdata/go-fb-test.fdb' USER 'gotest' PASSWORD 'gotest' PAGE_SIZE = 1024 DEFAULT CHARACTER SET NONE;"
)

func TestMapFromConnectionString(t *testing.T) {
	m, err := MapFromConnectionString(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error parsing connection string: %s", err)
	}
	if m["database"] != "localhost:/var/fbdata/go-fb-test.fdb" {
		t.Error("Error finding key: database")
	}
	if m["username"] != "gotest" {
		t.Error("Error finding key: database")
	}
	if m["password"] != "gotest" {
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
	if db.Username != "gotest" {
		t.Error("Username incorrect")
	}
	if db.Password != "gotest" {
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

func TestCreateStatement(t *testing.T) {
	db, _ := New(TestConnectionString)
	if db.CreateStatement() != CreateStatement {
		t.Errorf("Invalid CreateStatement: %s", db.CreateStatement())
	}
}

func FileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func ReadLine() (result string) {
	fmt.Println("Waiting...")
	result, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	return result
}

func TestDatabaseCreate(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	db, err := New(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	
	conn, err := db.Create()
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer conn.Close()
	if !FileExist(TestFilename) {
		t.Fatalf("Database was not created.")
	}
	if db != conn.database {
		t.Error("database field not set")
	}
}
