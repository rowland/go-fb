package fb

import (
	"testing"
	"os"
)

func TestFetch(t *testing.T) {
	const SqlSelect = "SELECT * FROM RDB$DATABASE"

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer conn.Drop()
	cursor, err := conn.Execute(SqlSelect)
	if err != nil {
		t.Fatalf("Error executing select statement: %s", err)
	}
	if cursor == nil {
		t.Fatal("Cursor should not be nil.")
	}
	var row []interface{}
	if err = cursor.Fetch(&row); err != nil {
		t.Fatalf("Error in Fetch: %s", err)
	}
	if len(row) != 4 {
		t.Errorf("Expected row length 4, found length %d", len(row))
	}
}
