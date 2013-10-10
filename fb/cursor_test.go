package fb

import (
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNextRow(t *testing.T) {
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
		t.Fatalf("Error in Next: %s", cursor.Err())
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

func TestScan(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := `CREATE TABLE TEST (
		ID BIGINT,
		FLAG INTEGER CHECK ((FLAG IN (0,1)) OR (FLAG IS NULL)),
		BINARY BLOB,
		I INTEGER,
		I32 INTEGER,
		I64 BIGINT,
		F32 FLOAT,
		F64 DOUBLE PRECISION,
		C CHAR,
		CS CHAR(26),
		V VARCHAR(1),
		VS VARCHAR(26),
		M BLOB SUB_TYPE TEXT,
		DT DATE,
		TM TIME,
		TS TIMESTAMP);`
	sqlInsert := `INSERT INTO TEST VALUES (
		1,
		1,
		'BINARY BLOB CONTENTS',
		123,
		1234,
		1234567890,
		123.24,
		123456789.24,
		'A',
		'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
		'A',
		'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
		'TEXT BLOB CONTENTS',
		'2013-10-10',
		'08:42:00',
		'2013-10-10 08:42:00');`
	sqlInsert2 := "INSERT INTO TEST (ID) VALUES (2);"
	sqlSelect := "SELECT * FROM TEST;"
	dtExpected := time.Date(2013, 10, 10, 0, 0, 0, 0, conn.Location)
	tmExpected := time.Date(1970, 1, 1, 8, 42, 0, 0, conn.Location)
	tsExpected := time.Date(2013, 10, 10, 8, 42, 0, 0, conn.Location)

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}
	if _, err = conn.Execute(sqlInsert2); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in Next: %v", cursor.Err())
	}

	var (
		id     int64
		flag   bool
		binary []byte
		i      int
		i32    int32
		i64    int64
		f32    float32
		f64    float64
		c      string
		cs     string
		v      string
		vs     string
		m      string
		dt     time.Time
		tm     time.Time
		ts     time.Time
	)

	var (
		nflag   NullableBool
		nbinary NullableBytes
		ni      NullableInt32
		ni32    NullableInt32
		ni64    NullableInt64
		nf32    NullableFloat32
		nf64    NullableFloat64
		nc      NullableString
		ncs     NullableString
		nv      NullableString
		nvs     NullableString
		nm      NullableString
		ndt     NullableTime
		ntm     NullableTime
		nts     NullableTime
	)

	if err = cursor.Scan(&id, &flag, &binary, &i, &i32, &i64, &f32, &f64, &c, &cs, &v, &vs, &m, &dt, &tm, &ts); err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Errorf("id: expected 1, got %v", id)
	}
	if flag != true {
		t.Errorf("flag: expected true, got %v", flag)
	}
	if string(binary) != "BINARY BLOB CONTENTS" {
		t.Errorf("binary: expected 'BINARY BLOB CONTENTS' got %v", binary)
	}
	if i != 123 {
		t.Errorf("i: expected 123, got %v", i)
	}
	if i32 != 1234 {
		t.Errorf("i32: expected 1234, got %v", i32)
	}
	if i64 != 1234567890 {
		t.Errorf("i64: expected 1234567890, got %v", i64)
	}
	if f32 != 123.24 {
		t.Errorf("f32: expected 123.24, got %v", f32)
	}
	if f64 != 123456789.24 {
		t.Errorf("f64: expected 123456789.24, got %v", f64)
	}
	if c != "A" {
		t.Errorf("c: expected 1, got %v", c)
	}
	if cs != "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		t.Errorf("cs: expected 1, got %v", cs)
	}
	if v != "A" {
		t.Errorf("v: expected 1, got %v", v)
	}
	if vs != "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		t.Errorf("vs: expected 1, got %v", vs)
	}
	if m != "TEXT BLOB CONTENTS" {
		t.Errorf("m: expected 'TEXT BLOB CONTENTS', got %v", m)
	}
	if dt != dtExpected {
		t.Errorf("dt: expected %v, got %v", dtExpected, dt)
	}
	if tm != tmExpected {
		t.Errorf("tm: expected %v, got %v", tmExpected, tm)
	}
	if ts != tsExpected {
		t.Errorf("ts: expected %v, got %v", tsExpected, ts)
	}

	if err = cursor.Scan(&id, &nflag, &nbinary, &ni, &ni32, &ni64, &nf32, &nf64, &nc, &ncs, &nv, &nvs, &nm, &ndt, &ntm, &nts); err != nil {
		t.Fatal(err)
	}

	if nflag.Null {
		t.Error("bool null")
	}
	if nbinary.Null {
		t.Error("bytes null")
	}
	if ni.Null {
		t.Error("int null")
	}
	if ni32.Null {
		t.Error("int32 null")
	}
	if ni64.Null {
		t.Error("int64 null")
	}
	if nf32.Null {
		t.Error("float32 null")
	}
	if nf64.Null {
		t.Error("float64 null")
	}
	if nc.Null {
		t.Error("char null")
	}
	if ncs.Null {
		t.Error("char string null")
	}
	if nv.Null {
		t.Error("varchar null")
	}
	if nvs.Null {
		t.Error("varchar string null")
	}
	if nm.Null {
		t.Error("memo null")
	}
	if ndt.Null {
		t.Error("date null")
	}
	if ntm.Null {
		t.Error("time null")
	}
	if nts.Null {
		t.Error("timestamp null")
	}

	if nflag.Value != true {
		t.Errorf("flag: expected true, got %v", nflag.Value)
	}
	if string(nbinary.Value) != "BINARY BLOB CONTENTS" {
		t.Errorf("binary: expected 'BINARY BLOB CONTENTS' got %v", nbinary.Value)
	}
	if ni.Value != 123 {
		t.Errorf("i: expected 123, got %v", ni.Value)
	}
	if ni32.Value != 1234 {
		t.Errorf("i32: expected 1234, got %v", ni32.Value)
	}
	if ni64.Value != 1234567890 {
		t.Errorf("i64: expected 1234567890, got %v", ni64.Value)
	}
	if nf32.Value != 123.24 {
		t.Errorf("f32: expected 123.24, got %v", nf32.Value)
	}
	if nf64.Value != 123456789.24 {
		t.Errorf("f64: expected 123456789.24, got %v", nf64.Value)
	}
	if nc.Value != "A" {
		t.Errorf("c: expected 1, got %v", nc.Value)
	}
	if ncs.Value != "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		t.Errorf("cs: expected 1, got %v", ncs.Value)
	}
	if nv.Value != "A" {
		t.Errorf("v: expected 1, got %v", nv.Value)
	}
	if nvs.Value != "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		t.Errorf("vs: expected 1, got %v", nvs.Value)
	}
	if nm.Value != "TEXT BLOB CONTENTS" {
		t.Errorf("m: expected 'TEXT BLOB CONTENTS', got %v", nm.Value)
	}
	if ndt.Value != dtExpected {
		t.Errorf("dt: expected %v, got %v", dtExpected, ndt.Value)
	}
	if ntm.Value != tmExpected {
		t.Errorf("tm: expected %v, got %v", tmExpected, ntm.Value)
	}
	if nts.Value != tsExpected {
		t.Errorf("ts: expected %v, got %v", tsExpected, nts.Value)
	}

	if !cursor.Next() {
		t.Fatalf("Error in Next: %v", cursor.Err())
	}
	if err = cursor.Scan(&id, &flag, &binary, &i, &i32, &i64, &f32, &f64, &c, &cs, &v, &vs, &m, &dt, &tm, &ts); err == nil {
		t.Fatal("Scan expected to fail")
	}

	if err = cursor.Scan(&id, &nflag, &nbinary, &ni, &ni32, &ni64, &nf32, &nf64, &nc, &ncs, &nv, &nvs, &nm, &ndt, &ntm, &nts); err != nil {
		t.Fatal(err)
	}
	if !nflag.Null {
		t.Error("bool not null")
	}
	if !nbinary.Null {
		t.Error("bytes not null")
	}
	if !ni.Null {
		t.Error("int not null")
	}
	if !ni32.Null {
		t.Error("int32 not null")
	}
	if !ni64.Null {
		t.Error("int64 not null")
	}
	if !nf32.Null {
		t.Error("float32 not null")
	}
	if !nf64.Null {
		t.Error("float64 not null")
	}
	if !nc.Null {
		t.Error("char not null")
	}
	if !ncs.Null {
		t.Error("char string not null")
	}
	if !nv.Null {
		t.Error("varchar not null")
	}
	if !nvs.Null {
		t.Error("varchar string not null")
	}
	if !nm.Null {
		t.Error("memo not null")
	}
	if !ndt.Null {
		t.Error("date not null")
	}
	if !ntm.Null {
		t.Error("time not null")
	}
	if !nts.Null {
		t.Error("timestamp not null")
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

func TestNextAfterEnd(t *testing.T) {
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
		t.Fatalf("Error in Next: %s", cursor.Err())
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

func TestNextAfterEnd2(t *testing.T) {
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
		t.Fatalf("Error in Next: %s", cursor.Err())
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
