package fb

import (
	"os"
	"strconv"
	"testing"
)

func TestTransactionStart(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if conn.TransactionStarted() {
		t.Fatal("No transaction should be started yet.")
	}
	if err := conn.TransactionStart(""); err != nil {
		t.Fatalf("Unexpected error starting 1st transaction: %s", err)
	}
	if !conn.TransactionStarted() {
		t.Fatal("1st transaction should be started.")
	}
	if err := conn.Commit(); err != nil {
		t.Fatalf("Unexpected error committing 1st transaction: %s", err)
	}
	if conn.TransactionStarted() {
		t.Fatal("1st transaction should no longer be started.")
	}
	if err := conn.TransactionStart(""); err != nil {
		t.Fatalf("Unexpected error starting 2nd transaction: %s", err)
	}
	if !conn.TransactionStarted() {
		t.Fatal("2nd transaction should be started.")
	}
	if err := conn.Rollback(); err != nil {
		t.Fatalf("Unexpected error rolling back 2nd transaction: %s", err)
	}
	if conn.TransactionStarted() {
		t.Fatal("2nd transaction should no longer be started.")
	}
}

func TestAutoTransactionInsertWithError(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	const sqlSchema = "CREATE TABLE TEST (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(20))"
	const sqlInsert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if conn.TransactionStarted() {
		t.Fatal("No transaction should be started by schema statement.")
	}

	if _, err = conn.Execute(sqlInsert, 1, "one"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	if conn.TransactionStarted() {
		t.Fatal("Auto transaction should already be committed.")
	}

	if _, err = conn.Execute(sqlInsert, 1, "two"); err == nil {
		t.Fatal("Expecting error executing insert.")
	}

	if conn.TransactionStarted() {
		t.Fatal("Auto transaction should not be left open.")
	}
}

func TestQueryInTransaction(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if conn.TransactionStarted() {
		t.Fatal("No transaction should be started.")
	}

	if err := conn.TransactionStart(""); err != nil {
		t.Fatalf("Unexpected error starting transaction: %s", err)
	}

	if !conn.TransactionStarted() {
		t.Fatal("Transaction should be started.")
	}

	cursor, err := conn.Execute("select * from rdb$database")
	if err != nil {
		t.Fatalf("Unexpected error executing query: %s", err)
	}
	defer cursor.Close()

	if !conn.TransactionStarted() {
		t.Fatal("Transaction should still be open.")
	}

	if err := conn.Commit(); err != nil {
		t.Fatalf("Unexpected error committing transaction: %s", err)
	}
	if conn.TransactionStarted() {
		t.Fatal("Transaction should no longer be open.")
	}
}

func TestInsertCommit(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	const sqlSchema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
	const sqlInsert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
	const sqlSelect = "SELECT * FROM TEST ORDER BY ID"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	if err := conn.TransactionStart(""); err != nil {
		t.Fatalf("Unexpected error starting transaction: %s", err)
	}
	for i := 0; i < 10; i++ {
		conn.Execute(sqlInsert, i, strconv.Itoa(i))
	}
	if err := conn.Commit(); err != nil {
		t.Fatalf("Unexpected error committing transaction: %s", err)
	}
	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()
	for i := 0; i < 10; i++ {
		if !cursor.Next() {
			t.Fatalf("Error in fetch: %s", cursor.Err())
		}
		vals := cursor.Row()
		if vals[0].(int32) != int32(i) {
			t.Fatalf("Expected %d, got %v", i, vals[0])
		}
		if vals[1].(string) != strconv.Itoa(i) {
			t.Fatalf("Expected %s, got %v", strconv.Itoa(i), vals[1])
		}
	}
	if cursor.Next() {
		t.Fatal("Expected error due to cursor being at end of data.")
	}
}

func TestInsertRollback(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	const sqlSchema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
	const sqlInsert = "INSERT INTO TEST (ID, NAME) VALUES (?, ?)"
	const sqlSelect = "SELECT * FROM TEST ORDER BY ID"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	if err := conn.TransactionStart(""); err != nil {
		t.Fatalf("Unexpected error starting transaction: %s", err)
	}
	for i := 0; i < 10; i++ {
		conn.Execute(sqlInsert, i, strconv.Itoa(i))
	}
	if err := conn.Rollback(); err != nil {
		t.Fatalf("Unexpected error rolling back transaction: %s", err)
	}
	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()
	if cursor.Next() {
		t.Fatal("Expected error due to cursor being at end of data.")
	}
}
