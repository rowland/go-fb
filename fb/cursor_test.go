package fb

import (
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
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
	if !cursor.Next() {
		t.Fatalf("Error in Fetch: %s", cursor.Err())
	}
	row := cursor.Row()
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

func TestCursorFieldsLowercased(t *testing.T) {
	const SqlSelect = "SELECT * FROM RDB$DATABASE"

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString + "lowercase_names=true;")
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
	st.Equal("rdb$description", fields[0].Name)
	st.Equal("rdb$relation_id", fields[1].Name)
	st.Equal("rdb$security_class", fields[2].Name)
	st.Equal("rdb$character_set_name", fields[3].Name)
}

func TestCursorFieldsMap(t *testing.T) {
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

	fields := cursor.FieldsMap
	if len(fields) != 4 {
		t.Fatalf("Expected 4 fields, found %d", len(fields))
	}
	st := SuperTest{t, "Fields"}
	st.Equal(520, fields["RDB$DESCRIPTION"].TypeCode)
	st.Equal(500, fields["RDB$RELATION_ID"].TypeCode)
	st.Equal(452, fields["RDB$SECURITY_CLASS"].TypeCode)
	st.Equal(452, fields["RDB$CHARACTER_SET_NAME"].TypeCode)
}

func TestCursorFieldsWithAliasedFields(t *testing.T) {
	const SqlSelect = "SELECT RDB$DESCRIPTION DES, RDB$RELATION_ID REL, RDB$SECURITY_CLASS SEC, RDB$CHARACTER_SET_NAME FROM RDB$DATABASE"

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
	st.Equal("DES", fields[0].Name)
	st.Equal("REL", fields[1].Name)
	st.Equal("SEC", fields[2].Name)
	st.Equal("RDB$CHARACTER_SET_NAME", fields[3].Name)
}

func TestFetchAfterEnd(t *testing.T) {
	const SqlCreateGen = "create generator test_seq"
	const SqlSelectGen = "select gen_id(test_seq, 1) from rdb$database"

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Error creating database: %s", err)
	}
	defer conn.Drop()
	_, err = conn.Execute(SqlCreateGen)
	if err != nil {
		t.Fatalf("Error executing create statement: %s", err)
	}

	cursor, err := conn.Execute(SqlSelectGen)
	if err != nil {
		t.Fatalf("Error executing select statement: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	if cursor.Next() {
		t.Fatal("Next should not succeed.")
	}
	if cursor.Err() != io.EOF {
		t.Fatalf("Expecting io.EOF, got: %s", err)
	}
	if cursor.Next() {
		t.Fatal("Next should not succeed.")
	}
	err2, ok := cursor.Err().(*Error)
	if !ok {
		t.Fatalf("Expecting fb.Error, got: %s", reflect.TypeOf(cursor.Err()))
	}
	if err2.Message != "Cursor is past end of data." {
		t.Errorf("Unexpected error message: %s", err2.Message)
	}
}

func TestFetchAfterEnd2(t *testing.T) {
	const SqlSelect = "select * from rdb$database"

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

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	if cursor.Next() {
		t.Fatal("Next should not succeed.")
	}
	if cursor.Err() != io.EOF {
		t.Fatalf("Expecting io.EOF, got: %s", err)
	}
	if cursor.Next() {
		t.Fatal("Next should not succeed.")
	}
	err2, ok := cursor.Err().(*Error)
	if !ok {
		t.Fatalf("Expecting fb.Error, got: %s", reflect.TypeOf(cursor.Err()))
	}
	if err2.Message != "Cursor is past end of data." {
		t.Errorf("Unexpected error message: %s", err2.Message)
	}
}
