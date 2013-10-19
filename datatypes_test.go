package fb

import (
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestInsertInteger(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 INTEGER, VAL2 INTEGER);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 500000, "500000"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	ni := NullableInt32{30303, false}
	ns := NullableString{"10203", false}
	if _, err = conn.Execute(sqlInsert, &ni, &ns); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	nni := NullableInt32{0, true}
	nns := NullableString{"", true}
	if _, err = conn.Execute(sqlInsert, &nni, &nns); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(int32(500000), vals[0])
	st.Equal(int32(500000), vals[1])

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	var ni2 NullableInt32
	var ns2 NullableString
	cursor.Scan(&ni2, &ns2)
	st.False(ni2.Null)
	st.Equal(ni, ni2)
	st.False(ns2.Null)
	st.Equal(ns, ns2)

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	var nni2 NullableInt32
	var nns2 NullableString
	cursor.Scan(&nni2, &nns2)
	st.True(nni2.Null)
	st.Equal(nni, nni2)
	st.True(nns2.Null)
	st.Equal(nns, nns2)
}

func TestInsertSmallint(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 SMALLINT, VAL2 SMALLINT);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 32123, "32123"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(int16(32123), vals[0])
	st.Equal(int16(32123), vals[1])
}

func TestInsertBigint(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 BIGINT, VAL2 BIGINT);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 5000000000, "5000000000"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(int64(5000000000), vals[0])
	st.Equal(int64(5000000000), vals[1])
}

func TestInsertFloat(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 FLOAT, VAL2 FLOAT);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 5.75, "5.75"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(float32(5.75), vals[0])
	st.Equal(float32(5.75), vals[1])
}

func TestInsertDouble(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 DOUBLE PRECISION, VAL2 DOUBLE PRECISION);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 12345.12345, "12345.12345"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(float64(12345.12345), vals[0])
	st.Equal(float64(12345.12345), vals[1])
}

func TestInsertChar(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 CHAR, VAL10 CHAR(10));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL10) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, "5", "1234567890"); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, 5, 1234567890); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal("5", vals[0])
	st.Equal("1234567890", vals[1])

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals = cursor.Row()
	st.Equal("5", vals[0])
	st.Equal("1234567890", vals[1])
}

func TestInsertVarchar(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 VARCHAR(1), VAL10 VARCHAR(10));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL10) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, "5", "1234567890"); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, 5, 1234567890); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal("5", vals[0])
	st.Equal("1234567890", vals[1])

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals = cursor.Row()
	st.Equal("5", vals[0])
	st.Equal("1234567890", vals[1])
}

func TestInsertVarchar10000(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 VARCHAR(10000), VAL2 VARCHAR(10000));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	bs := strings.Repeat("1", 100)
	bi, _ := new(big.Int).SetString(bs, 10)

	if _, err = conn.Execute(sqlInsert, bs, bi); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(bs, vals[0])
	st.Equal(bs, vals[1])
}

func TestInsertTimestamp(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 TIMESTAMP, VAL2 TIMESTAMP, VAL3 TIMESTAMP, VAL4 TIMESTAMP);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2, VAL3, VAL4) VALUES (?, ?, ?, '2006/6/6 3:33:33');"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	dt := time.Date(2006, 6, 6, 3, 33, 33, 0, conn.Location)
	dt2 := "2006/6/6 3:33:33"
	dt3 := "2006-6-6 3:33:33"

	if _, err = conn.Execute(sqlInsert, dt, dt2, dt3); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(dt, vals[0])
	st.Equal(dt, vals[1])
	st.Equal(dt, vals[2])
	st.Equal(dt, vals[3])
}

func TestInsertDate(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 DATE, VAL2 DATE, VAL3 DATE, VAL4 DATE);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2, VAL3, VAL4) VALUES (?, ?, ?, '2006/6/6');"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	dt := time.Date(2006, 6, 6, 0, 0, 0, 0, conn.Location)
	dt2 := "2006/6/6"
	dt3 := "2006-6-6"

	if _, err = conn.Execute(sqlInsert, dt, dt2, dt3); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(dt, vals[0])
	st.Equal(dt, vals[1])
	st.Equal(dt, vals[2])
	st.Equal(dt, vals[3])
}

func TestInsertTime(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 TIME, VAL2 TIME, VAL3 TIME);"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2, VAL3) VALUES (?, ?, '3:33:33');"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	dt := time.Date(1970, 1, 1, 3, 33, 33, 0, conn.Location)
	dt2 := "3:33:33"

	if _, err = conn.Execute(sqlInsert, dt, dt2); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(dt, vals[0])
	st.Equal(dt, vals[1])
	st.Equal(dt, vals[2])
}

func TestInsertNumeric(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 NUMERIC(9,2), VAL2 NUMERIC(15,4), VAL3 NUMERIC(3,1));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2, VAL3) VALUES (?, ?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 12345.12, 12345.1234, 12.1); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "12345.12", "12345.1234", "12.1"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(float64(12345.12), vals[0])
	st.Equal(float64(12345.1234), vals[1])
	st.Equal(float64(12.1), vals[2])

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals = cursor.Row()
	st.Equal(float64(12345.12), vals[0])
	st.Equal(float64(12345.1234), vals[1])
	st.Equal(float64(12.1), vals[2])
}

func TestInsertDecimal(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 DECIMAL(9,2), VAL2 DECIMAL(15,4), VAL3 DECIMAL(3,1));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL2, VAL3) VALUES (?, ?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	if _, err = conn.Execute(sqlInsert, 12345.12, 12345.1234, 12.1); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "12345.12", "12345.1234", "12.1"); err != nil {
		t.Fatalf("Error executing insert: %s", err)
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals := cursor.Row()
	st.Equal(float64(12345.12), vals[0])
	st.Equal(float64(12345.1234), vals[1])
	st.Equal(float64(12.1), vals[2])

	if !cursor.Next() {
		t.Fatalf("Error in fetch: %s", cursor.Err())
	}
	vals = cursor.Row()
	st.Equal(float64(12345.12), vals[0])
	st.Equal(float64(12345.1234), vals[1])
	st.Equal(float64(12.1), vals[2])
}

func TestInsertBlob(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (ID INT, NAME VARCHAR(20), MEMO BLOB SUB_TYPE TEXT, BINARY BLOB)"
	sqlInsert := "INSERT INTO TEST (ID, NAME, MEMO, BINARY) VALUES (?, ?, ?, ?)"
	sqlSelect := "SELECT * FROM TEST ORDER BY ID"

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	sentence := "The quick red fox jumps over the lazy brown dog.\n"
	memo := strings.Repeat(sentence, 1000)

	for id := 0; id < 5; id++ {
		if _, err = conn.Execute(sqlInsert, id, strconv.Itoa(id), memo, memo); err != nil {
			t.Fatalf("Error executing insert: %s", err)
		}
	}
	if conn.TransactionStarted() {
		t.Error("Should not be in transaction here.")
	}

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	for id := 0; id < 5; id++ {
		if !cursor.Next() {
			t.Fatalf("Error in fetch: %s", cursor.Err())
		}
		vals := cursor.Row()
		st.Equal(int32(id), vals[0])
		name := strconv.Itoa(id)
		st.Equal(name, vals[1])
		st.Equal(memo, vals[2])
		st.Equal(memo, string(vals[3].([]byte)))
	}
}
