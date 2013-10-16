package fb

import (
	"os"
	"testing"
)

func TestExecute(t *testing.T) {
	const SqlSchema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
	const SqlSelect = "SELECT * FROM RDB$DATABASE"

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if conn.TransactionStarted() {
		t.Fatal("Transaction should not be started before a statement is executed.")
	}
	if _, err := conn.Execute(SqlSchema); err != nil {
		t.Fatalf("Unexpected error executing schema statment: %s", err)
	}
	if _, err := conn.Execute(SqlSelect); err != nil {
		t.Fatalf("Unexpected error executing select statment: %s", err)
	}
	if !conn.TransactionStarted() {
		t.Error("Transaction should be started")
	}
	if err := conn.Commit(); err != nil {
		t.Fatalf("Unexpected error committing transaction: %s", err)
	}
	if conn.TransactionStarted() {
		t.Fatal("Transaction should not be started after transaction is committed.")
	}
}

func TestTableNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = "CREATE TABLE TEST1 (ID INTEGER); CREATE TABLE TEST2 (ID INTEGER);"
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tableNames, err := conn.TableNames()
	st.MustEqual(2, len(tableNames))
	st.Equal("TEST1", tableNames[0])
	st.Equal("TEST2", tableNames[1])
}

func TestTableNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = "CREATE TABLE TEST1 (ID INTEGER); CREATE TABLE TEST2 (ID INTEGER);"
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString + "lowercase_names=true;")
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tableNames, err := conn.TableNames()
	st.MustEqual(2, len(tableNames))
	st.Equal("test1", tableNames[0])
	st.Equal("test2", tableNames[1])
}
