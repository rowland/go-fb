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

	var cursor *Cursor
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int32) != 500000 {
		t.Errorf("(0) Expected %d, got %d", 500000, vals[0])
	}
	if vals[1].(int32) != 500000 {
		t.Fatalf("(1) Expected %d, got %d", 500000, vals[1])
	}
}

func TestInsertSmallint(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int16) != 32123 {
		t.Fatalf("(0) Expected %d, got %d", 32123, vals[0])
	}
	if vals[1].(int16) != 32123 {
		t.Fatalf("(1) Expected %d, got %d", 32123, vals[1])
	}
}

func TestInsertBigint(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int64) != 5000000000 {
		t.Errorf("(0) Expected %d, got %d", 5000000000, vals[0])
	}
	if vals[1].(int64) != 5000000000 {
		t.Fatalf("(1) Expected %d, got %d", 5000000000, vals[1])
	}
}

func TestInsertFloat(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float32) != 5.75 {
		t.Fatalf("(0) Expected %f, got %f", 5.75, vals[0])
	}
	if vals[1].(float32) != 5.75 {
		t.Fatalf("(1) Expected %f, got %f", 5.75, vals[1])
	}
}

func TestInsertDouble(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12345 {
		t.Fatalf("(0) Expected %f, got %f", 12345.12345, vals[0])
	}
	if vals[1].(float64) != 12345.12345 {
		t.Fatalf("(1) Expected %f, got %f", 12345.12345, vals[1])
	}
}

func TestInsertChar(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("(0) Expected %s, got %s", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("(1) Expected %s, got %s", "1234567890", vals[1])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("(0) Expected %d, got %d", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("(1) Expected %s, got %s", "1234567890", vals[1])
	}
}

func TestInsertVarchar(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("(0) Expected %s, got %s", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("(1) Expected %s, got %s", "1234567890", vals[1])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("(0) Expected %d, got %d", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("(1) Expected %s, got %s", "1234567890", vals[1])
	}
}

func TestInsertVarchar10000(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != bs {
		t.Fatalf("(0) Expected %s, got %s", bs, vals[0])
	}
	if vals[1].(string) != bs {
		t.Fatalf("(1) Expected %d, got %d", bs, vals[1])
	}
}

func TestInsertTimestamp(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(0) Expected %s, got %s", dt, vals[0])
	}
	if !vals[1].(time.Time).Equal(dt) {
		t.Fatalf("(1) Expected %s, got %s", dt, vals[1])
	}
	if !vals[2].(time.Time).Equal(dt) {
		t.Fatalf("(2) Expected %s, got %s", dt, vals[2])
	}
	if !vals[3].(time.Time).Equal(dt) {
		t.Fatalf("(3) Expected %s, got %s", dt, vals[3])
	}
}

func TestInsertDate(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(0) Expected %s, got %s", dt, vals[0])
	}
	if !vals[1].(time.Time).Equal(dt) {
		t.Fatalf("(1) Expected %s, got %s", dt, vals[1])
	}
	if !vals[2].(time.Time).Equal(dt) {
		t.Fatalf("(2) Expected %s, got %s", dt, vals[2])
	}
	if !vals[3].(time.Time).Equal(dt) {
		t.Fatalf("(3) Expected %s, got %s", dt, vals[3])
	}
}

func TestInsertTime(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(0) Expected %s, got %s", dt, vals[0])
	}
	if !vals[1].(time.Time).Equal(dt) {
		t.Fatalf("(1) Expected %s, got %s", dt, vals[1])
	}
	if !vals[2].(time.Time).Equal(dt) {
		t.Fatalf("(2) Expected %s, got %s", dt, vals[2])
	}
}

func TestInsertNumeric(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12 {
		t.Fatalf("(0) Expected %f, got %v", 12345.12, vals[0])
	}
	if vals[1].(float64) != 12345.1234 {
		t.Fatalf("(1) Expected %f, got %v", 12345.1234, vals[1])
	}
	if vals[2].(float64) != 12.1 {
		t.Fatalf("(2) Expected %f, got %v", 12.1, vals[2])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12 {
		t.Fatalf("(0) Expected %f, got %v", 12345.12, vals[0])
	}
	if vals[1].(float64) != 12345.1234 {
		t.Fatalf("(1) Expected %f, got %v", 12345.1234, vals[1])
	}
	if vals[2].(float64) != 12.1 {
		t.Fatalf("(2) Expected %f, got %v", 12.1, vals[2])
	}
}

func TestInsertDecimal(t *testing.T) {
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

	var vals []interface{}
	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12 {
		t.Fatalf("(0) Expected %f, got %v", 12345.12, vals[0])
	}
	if vals[1].(float64) != 12345.1234 {
		t.Fatalf("(1) Expected %f, got %v", 12345.1234, vals[1])
	}
	if vals[2].(float64) != 12.1 {
		t.Fatalf("(2) Expected %f, got %v", 12.1, vals[2])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12 {
		t.Fatalf("(0) Expected %f, got %v", 12345.12, vals[0])
	}
	if vals[1].(float64) != 12345.1234 {
		t.Fatalf("(1) Expected %f, got %v", 12345.1234, vals[1])
	}
	if vals[2].(float64) != 12.1 {
		t.Fatalf("(2) Expected %f, got %v", 12.1, vals[2])
	}
}

func TestInsertBlob(t *testing.T) {
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
		var vals []interface{}
		if err = cursor.Fetch(&vals); err != nil {
			t.Fatalf("Error in fetch: %s", err)
		}
		if vals[0].(int32) != int32(id) {
			t.Fatalf("(0) Expected %d, got %v", id, vals[0])
		}
		name := strconv.Itoa(id)
		if vals[1].(string) != name {
			t.Fatalf("(1) Expected %s, got %v", name, vals[1])
		}
		if vals[2].(string) != memo {
			t.Fatalf("(2) Expected %s, got %v", memo, vals[2])
		}
		if string(vals[3].([]byte)) != memo {
			t.Fatalf("(3) Expected %s, got %v", memo, vals[3])
		}
	}
}
