package fb

import (
	"testing"
	"os"
	"bufio"
	"fmt"
)

const (
	TestFilename          = "/var/fbdata/go-fb-test.fdb"
	TestConnectionString  = "database=localhost:/var/fbdata/go-fb-test.fdb; username=gotest; password=gotest; charset=NONE; role=READER;"
	TestConnectionString2 = "database=localhost:/var/fbdata/go-fb-test.fdb;username=gotest;password=gotest;lowercase_names=true;page_size=2048"
	TestConnectionString3 = "database=localhost:/var/fbdata/go-fb-test.fdb;username=bogus;password=gotest;lowercase_names=true;page_size=2048"
	CreateStatement       = "CREATE DATABASE 'localhost:/var/fbdata/go-fb-test.fdb' USER 'gotest' PASSWORD 'gotest' PAGE_SIZE = 1024 DEFAULT CHARACTER SET NONE;"
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
	st := SuperTest{t, "TestNew"}
	st.Equal("localhost:/var/fbdata/go-fb-test.fdb", db.Database)
	st.Equal("gotest", db.Username)
	st.Equal("gotest", db.Password)
	st.Equal("NONE", db.Charset)
	st.Equal("READER", db.Role)
}

func TestNew2(t *testing.T) {
	db, err := New(TestConnectionString2)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	st := SuperTest{t, "TestNew2"}
	st.Equal("", db.Charset)
	st.Equal("", db.Role)
	st.Equal(true, db.LowercaseNames)
	st.Equal(2048, db.PageSize)
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

func TestCreate(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer conn.Close()
	if !FileExist(TestFilename) {
		t.Fatalf("Database was not created.")
	}
}

const CreateErrorMessage = `Unsuccessful execution caused by a system error that precludes successful execution of subsequent statements
Your user name and password are not defined. Ask your database administrator to set up a Firebird login.
`

func TestCreate2(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	conn, err := Create(TestConnectionString3)
	if err == nil {
		t.Error("Expected error creating database.")
	}
	if err.String() != CreateErrorMessage {
		t.Logf("Unexpected error message: %s", err)
		t.Logf("Expected message: %s", CreateErrorMessage)
		t.Fail()
	}
	if conn != nil {
		defer conn.Close()
		t.Error("Connection should be nil")
	}
	if FileExist(TestFilename) {
		t.Error("Database was created with bogus credentials.")
	}
}

func TestDatabaseConnect(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	db, err := New(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	conn, err := db.Create()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	conn.Close()

	conn, err = db.Connect()
	if err != nil {
		t.Fatalf("Error connecting to database: %s", err)
	}
	conn.Close()
}

func TestConnect(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	conn.Close()

	conn, err = Connect(TestConnectionString)
	if err != nil {
		t.Fatalf("Error connecting to database: %s", err)
	}
	conn.Close()
}

func TestDatabaseDrop(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	if FileExist(TestFilename) {
		t.Fatal("Database should not exist.")
	}

	db, err := New(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	conn, err := db.Create()
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	conn.Close()

	if !FileExist(TestFilename) {
		t.Fatalf("Database was not created.")
	}

	if err = db.Drop(); err != nil {
		t.Fatalf("Error dropping database: %s", err)
	}

	if FileExist(TestFilename) {
		t.Fatal("Database should not exist after being dropped.")
	}
}

func TestDrop(t *testing.T) {
	os.Remove(TestFilename)
	defer os.Remove(TestFilename)

	if FileExist(TestFilename) {
		t.Fatal("Database should not exist.")
	}

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	conn.Close()

	if !FileExist(TestFilename) {
		t.Fatalf("Database was not created.")
	}

	if err = Drop(TestConnectionString); err != nil {
		t.Fatalf("Error dropping database: %s", err)
	}

	if FileExist(TestFilename) {
		t.Fatal("Database should not exist after being dropped.")
	}
}
