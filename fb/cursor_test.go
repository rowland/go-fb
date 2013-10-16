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
	st := SuperTest{t}
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
	st.Equal(4, len(row))
	st.Equal(nil, row[0])
	_, ok := row[1].(int16)
	if !ok {
		t.Errorf("Expected row[1] to be an int, got %v", reflect.TypeOf(row[1]))
	}
	st.Equal(nil, row[2])
	st.Equal("NONE", strings.TrimSpace(row[3].(string)))
}

var sqlSampleSchema = `CREATE TABLE TEST (
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
var sqlSampleInsert = `INSERT INTO TEST VALUES (
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

func TestRowMap(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlInsert2 := "INSERT INTO TEST (ID) VALUES (2);"
	sqlSelect := "SELECT * FROM TEST;"
	dtExpected := time.Date(2013, 10, 10, 0, 0, 0, 0, conn.Location)
	tmExpected := time.Date(1970, 1, 1, 8, 42, 0, 0, conn.Location)
	tsExpected := time.Date(2013, 10, 10, 8, 42, 0, 0, conn.Location)

	if _, err = conn.Execute(sqlSampleSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlSampleInsert); err != nil {
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
	row := cursor.RowMap()

	st.Equal(int64(1), row["ID"])
	st.Equal(int32(1), row["FLAG"])
	st.Equal("BINARY BLOB CONTENTS", string(row["BINARY"].([]byte)))
	st.Equal(int32(123), row["I"])
	st.Equal(int32(1234), row["I32"])
	st.Equal(int64(1234567890), row["I64"])
	st.Equal(float32(123.24), row["F32"])
	st.Equal(123456789.24, row["F64"])
	st.Equal("A", row["C"])
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", row["CS"])
	st.Equal("A", row["V"])
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", row["VS"])
	st.Equal("TEXT BLOB CONTENTS", row["M"])
	st.Equal(dtExpected, row["DT"])
	st.Equal(tmExpected, row["TM"])
	st.Equal(tsExpected, row["TS"])

	if !cursor.Next() {
		t.Fatalf("Error in Next: %v", cursor.Err())
	}
	row = cursor.RowMap()

	st.Nil(row["FLAG"])
	st.Nil(row["BINARY"])
	st.Nil(row["I"])
	st.Nil(row["I32"])
	st.Nil(row["I64"])
	st.Nil(row["F32"])
	st.Nil(row["F64"])
	st.Nil(row["C"])
	st.Nil(row["CS"])
	st.Nil(row["V"])
	st.Nil(row["VS"])
	st.Nil(row["M"])
	st.Nil(row["DT"])
	st.Nil(row["TM"])
	st.Nil(row["TS"])
}

func TestScan(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlInsert2 := "INSERT INTO TEST (ID) VALUES (2);"
	sqlSelect := "SELECT * FROM TEST;"
	dtExpected := time.Date(2013, 10, 10, 0, 0, 0, 0, conn.Location)
	tmExpected := time.Date(1970, 1, 1, 8, 42, 0, 0, conn.Location)
	tsExpected := time.Date(2013, 10, 10, 8, 42, 0, 0, conn.Location)

	if _, err = conn.Execute(sqlSampleSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlSampleInsert); err != nil {
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

	st.Equal(int64(1), id)
	st.Equal(true, flag)
	st.Equal("BINARY BLOB CONTENTS", string(binary))
	st.Equal(123, i)
	st.Equal(int32(1234), i32)
	st.Equal(int64(1234567890), i64)
	st.Equal(float32(123.24), f32)
	st.Equal(123456789.24, f64)
	st.Equal("A", c)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", cs)
	st.Equal("A", v)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", vs)
	st.Equal("TEXT BLOB CONTENTS", m)
	st.Equal(dtExpected, dt)
	st.Equal(tmExpected, tm)
	st.Equal(tsExpected, ts)

	if err = cursor.Scan(&id, &nflag, &nbinary, &ni, &ni32, &ni64, &nf32, &nf64, &nc, &ncs, &nv, &nvs, &nm, &ndt, &ntm, &nts); err != nil {
		t.Fatal(err)
	}

	st.False(nflag.Null)
	st.False(nbinary.Null)
	st.False(ni.Null)
	st.False(ni32.Null)
	st.False(ni64.Null)
	st.False(nf32.Null)
	st.False(nf64.Null)
	st.False(nc.Null)
	st.False(ncs.Null)
	st.False(nv.Null)
	st.False(nvs.Null)
	st.False(nm.Null)
	st.False(ndt.Null)
	st.False(ntm.Null)
	st.False(nts.Null)

	st.Equal(true, nflag.Value)
	st.Equal("BINARY BLOB CONTENTS", string(nbinary.Value))
	st.Equal(int32(123), ni.Value)
	st.Equal(int32(1234), ni32.Value)
	st.Equal(int64(1234567890), ni64.Value)
	st.Equal(float32(123.24), nf32.Value)
	st.Equal(123456789.24, nf64.Value)
	st.Equal("A", nc.Value)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", ncs.Value)
	st.Equal("A", nv.Value)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", nvs.Value)
	st.Equal("TEXT BLOB CONTENTS", nm.Value)
	st.Equal(dtExpected, ndt.Value)
	st.Equal(tmExpected, ntm.Value)
	st.Equal(tsExpected, nts.Value)

	if !cursor.Next() {
		t.Fatalf("Error in Next: %v", cursor.Err())
	}
	if err = cursor.Scan(&id, &flag, &binary, &i, &i32, &i64, &f32, &f64, &c, &cs, &v, &vs, &m, &dt, &tm, &ts); err == nil {
		t.Fatal("Scan expected to fail")
	}

	if err = cursor.Scan(&id, &nflag, &nbinary, &ni, &ni32, &ni64, &nf32, &nf64, &nc, &ncs, &nv, &nvs, &nm, &ndt, &ntm, &nts); err != nil {
		t.Fatal(err)
	}
	st.True(nflag.Null)
	st.True(nbinary.Null)
	st.True(ni.Null)
	st.True(ni32.Null)
	st.True(ni64.Null)
	st.True(nf32.Null)
	st.True(nf64.Null)
	st.True(nc.Null)
	st.True(ncs.Null)
	st.True(nv.Null)
	st.True(nvs.Null)
	st.True(nm.Null)
	st.True(ndt.Null)
	st.True(ntm.Null)
	st.True(nts.Null)
}

func TestCursorFields(t *testing.T) {
	st := SuperTest{t}
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
	st.Equal(4, len(fields))
	st.Equal("RDB$DESCRIPTION", fields[0].Name)
	st.Equal("RDB$RELATION_ID", fields[1].Name)
	st.Equal("RDB$SECURITY_CLASS", fields[2].Name)
	st.Equal("RDB$CHARACTER_SET_NAME", fields[3].Name)
}

func TestCursorFieldsLowercased(t *testing.T) {
	st := SuperTest{t}
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
	st.Equal(4, len(fields))
	st.Equal("rdb$description", fields[0].Name)
	st.Equal("rdb$relation_id", fields[1].Name)
	st.Equal("rdb$security_class", fields[2].Name)
	st.Equal("rdb$character_set_name", fields[3].Name)
}

func TestCursorFieldsMap(t *testing.T) {
	st := SuperTest{t}
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
	st.Equal(4, len(fields))
	st.Equal(520, fields["RDB$DESCRIPTION"].TypeCode)
	st.Equal(500, fields["RDB$RELATION_ID"].TypeCode)
	st.Equal(452, fields["RDB$SECURITY_CLASS"].TypeCode)
	st.Equal(452, fields["RDB$CHARACTER_SET_NAME"].TypeCode)
}

func TestCursorFieldsWithAliasedFields(t *testing.T) {
	st := SuperTest{t}
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
	st.Equal(4, len(fields))
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
