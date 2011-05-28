package fb

import (
	"testing"
	"os"
	"strings"
	"reflect"
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
	if row[0] != nil {
		t.Errorf("Expected row[0] to be nil, got %v", row[0])
	}
	_, ok := row[1].(int16)
	if !ok {
		t.Errorf("Expected row[1] to be an int, got %v", reflect.TypeOf(row[1]))
	}
	if row[2] != nil {
		t.Errorf("Expected row[2] to be nil, got %v", row[2])
	}
	if strings.TrimSpace(row[3].(string)) != "NONE" {
		t.Errorf("Expected row[3] to be 'NONE', got '%v'", row[3])
	}
}

func TestCursorFields(t *testing.T) {
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
	defer cursor.Close()

	fields := cursor.Fields
	if len(fields) != 4 {
		t.Fatalf("Expected 4 fields, found %d", len(fields))
	}
	st := SuperTest{t, "Fields"}
	st.Equal("RDB$DESCRIPTION", fields[0].Name)
	st.Equal("RDB$RELATION_ID", fields[1].Name)
	st.Equal("RDB$SECURITY_CLASS", fields[2].Name)
	st.Equal("RDB$CHARACTER_SET_NAME", fields[3].Name)
}
