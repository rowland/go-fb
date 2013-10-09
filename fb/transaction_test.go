package fb

import (
	"os"
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
