package fb

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
	// "bytes"
	"os"
)

func genI(i int) int32 {
	return int32(i)
}

func genSi(i int) int16 {
	return int16(i)
}

func genBi(i int) int64 {
	return int64(i) * 1000000000
}

func genF(i int) float32 {
	return float32(i) / 2
}

func genD(i int) float64 {
	return float64(i) * 3333 / 2
}

func genC(i int) string {
	return fmt.Sprintf("%c", i+64)
}

func genC10(i int) string {
	return strings.Repeat(genC(i), 5)
}

func genVc(i int) string {
	return genC(i)
}

func genVc10(i int) string {
	return strings.Repeat(genC(i), i)
}

func genVc10000(i int) string {
	return strings.Repeat(genC(i), i*1000)
}

func genDt(i int) time.Time {
	return time.Date(2000, time.Month(i+1), i+1, 0, 0, 0, 0, time.Local)
}

func genTm(i int) time.Time {
	return time.Date(1990, time.Month(1), 1, 12, i, i, 0, time.Local)
}

func genTs(i int) time.Time {
	return time.Date(2006, time.Month(1), 1, i, i, i, 0, time.Local)
}

func genN92(i int) float64 {
	return float64(i) * 100
}

func genD92(i int) float64 {
	return float64(i) * 100
}

func TestGenI(t *testing.T) {
	if genI(3) != 3 {
		t.Errorf("Expected: %d, got: %d", 3, genI(3))
	}
}

func TestGenSi(t *testing.T) {
	if genSi(3) != 3 {
		t.Errorf("Expected: %d, got: %d", 3, genSi(3))
	}
}

func TestGenBi(t *testing.T) {
	if genBi(3) != 3*1000000000 {
		t.Errorf("Expected: %d, got: %d", int64(3)*1000000000, genBi(3))
	}
}

func TestGenF(t *testing.T) {
	if genF(3) != 1.5 {
		t.Errorf("Expected: %f, got: %f", 1.5, genF(3))
	}
}

func TestGenD(t *testing.T) {
	if genD(3) != 4999.5 {
		t.Errorf("Expected: %f, got: %f", 4999.5, genD(3))
	}
}

func TestInsertInteger(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL INTEGER);"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, 500000); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "500000"); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int32) != 500000 {
		t.Errorf("Expected %d, got %d", 500000, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int32) != 500000 {
		t.Fatalf("Expected %d, got %d", 500000, vals[0])
	}
}

func TestInsertSmallint(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL SMALLINT);"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, 32123); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "32123"); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int16) != 32123 {
		t.Fatalf("Expected %d, got %d", 32123, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(int16) != 32123 {
		t.Fatalf("Expected %d, got %d", 32123, vals[0])
	}
}

func TestInsertFloat(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL FLOAT);"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, 5.75); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "5.75"); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float32) != 5.75 {
		t.Fatalf("Expected %f, got %f", 5.75, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float32) != 5.75 {
		t.Fatalf("Expected %f, got %f", 5.75, vals[0])
	}
}

func TestInsertDouble(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL DOUBLE PRECISION);"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, 12345.12345); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, "12345.12345"); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12345 {
		t.Fatalf("Expected %f, got %f", 12345.12345, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(float64) != 12345.12345 {
		t.Fatalf("Expected %f, got %f", 12345.12345, vals[0])
	}
}

func TestInsertChar(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL1 CHAR, VAL10 VARCHAR(10));"
	sqlInsert := "INSERT INTO TEST (VAL1, VAL10) VALUES (?, ?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, "5", "1234567890"); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, 5, 1234567890); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("Expected %s, got %s", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("Expected %s, got %s", "1234567890", vals[1])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("Expected %d, got %d", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("Expected %s, got %s", "1234567890", vals[1])
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

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	if _, err = conn.Execute(sqlInsert, "5", "1234567890"); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, 5, 1234567890); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("Expected %s, got %s", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("Expected %s, got %s", "1234567890", vals[1])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != "5" {
		t.Fatalf("Expected %d, got %d", "5", vals[0])
	}
	if vals[1].(string) != "1234567890" {
		t.Fatalf("Expected %s, got %s", "1234567890", vals[1])
	}
}

func TestInsertVarchar10000(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL VARCHAR(10000));"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	bs := strings.Repeat("1", 100)
	bi, _ := new(big.Int).SetString(bs, 10)

	if _, err = conn.Execute(sqlInsert, bs); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, bi); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != bs {
		t.Fatalf("Expected %s, got %s", bs, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if vals[0].(string) != bs {
		t.Fatalf("Expected %d, got %d", bs, vals[0])
	}
}

func TestInsertTimestamp(t *testing.T) {
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSchema := "CREATE TABLE TEST (VAL TIMESTAMP);"
	sqlInsert := "INSERT INTO TEST (VAL) VALUES (?);"
	sqlSelect := "SELECT * FROM TEST;"

	var cursor *Cursor

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}
	conn.Commit()

	dt := time.Date(2006, 6, 6, 3, 33, 33, 0, conn.Location)
	dt2 := "2006/6/6 3:33:33"
	dt3 := "2006-6-6 3:33:33"
	sqlInsert4 := "INSERT INTO TEST (VAL) VALUES ('2006/6/6 3:33:33');"

	if _, err = conn.Execute(sqlInsert, dt); err != nil {
		t.Fatalf("Error executing insert (1): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, dt2); err != nil {
		t.Fatalf("Error executing insert (2): %s", err)
	}
	if _, err = conn.Execute(sqlInsert, dt3); err != nil {
		t.Fatalf("Error executing insert (3): %s", err)
	}
	if _, err = conn.Execute(sqlInsert4); err != nil {
		t.Fatalf("Error executing insert (4): %s", err)
	}

	var vals []interface{}
	if cursor, err = conn.Execute(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}
	defer cursor.Close()

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(1) Expected %s, got %s", dt, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(2) Expected %s, got %s", dt, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(3) Expected %s, got %s", dt, vals[0])
	}

	if err = cursor.Fetch(&vals); err != nil {
		t.Fatalf("Error in fetch: %s", err)
	}
	if !vals[0].(time.Time).Equal(dt) {
		t.Fatalf("(4) Expected %s, got %s", dt, vals[0])
	}
}

// cols := []string{"BI", "DT", "TM", "N92", "D92", "N154"}
// types := []string{"BIGINT", "DATE", "TIME", "NUMERIC(9,2)", "DECIMAL(9,2)", "NUMERIC(15,4)"}

/*
  Database.create(@parms) do |connection|
    connection.execute_script(sql_schema)
    cols.size.times do |i|
      sql_insert = "INSERT INTO TEST_#{cols[i]} (VAL) VALUES (?);"
      sql_select = "SELECT * FROM TEST_#{cols[i]};"
      if cols[i] == 'I'
        connection.execute(sql_insert, 500_000)
        connection.execute(sql_insert, "500_000")
        vals = connection.query(sql_select)
        assert_equal 500_000, vals[0][0]
        assert_equal 500_000, vals[1][0]
      elsif cols[i] == 'SI'
        connection.execute(sql_insert, 32_123)
        connection.execute(sql_insert, "32_123")
        vals = connection.query(sql_select)
        assert_equal 32_123, vals[0][0]
        assert_equal 32_123, vals[1][0]
      elsif cols[i] == 'BI'
        connection.execute(sql_insert, 5_000_000_000)
        connection.execute(sql_insert, "5_000_000_000")
        vals = connection.query(sql_select)
        assert_equal 5_000_000_000, vals[0][0]
        assert_equal 5_000_000_000, vals[1][0]
      elsif cols[i] == 'F'
        connection.execute(sql_insert, 5.75)
        connection.execute(sql_insert, "5.75")
        vals = connection.query(sql_select)
        assert_equal 5.75, vals[0][0]
        assert_equal 5.75, vals[1][0]
      elsif cols[i] == 'D'
        connection.execute(sql_insert, 12345.12345)
        connection.execute(sql_insert, "12345.12345")
        vals = connection.query(sql_select)
        assert_equal 12345.12345, vals[0][0]
        assert_equal 12345.12345, vals[1][0]
      elsif cols[i] == 'VC'
        connection.execute(sql_insert, "5")
        connection.execute(sql_insert, 5)
        vals = connection.query(sql_select)
        assert_equal "5", vals[0][0]
        assert_equal "5", vals[1][0]
      elsif cols[i] ==  'VC10'
        connection.execute(sql_insert, "1234567890")
        connection.execute(sql_insert, 1234567890)
        vals = connection.query(sql_select)
        assert_equal "1234567890", vals[0][0]
        assert_equal "1234567890", vals[1][0]
      elsif cols[i].include?('VC10000')
        connection.execute(sql_insert, "1" * 100)
        connection.execute(sql_insert, ("1" * 100).to_i)
        vals = connection.query(sql_select)
        assert_equal "1" * 100, vals[0][0]
        assert_equal "1" * 100, vals[1][0]
      elsif cols[i] == 'C'
        connection.execute(sql_insert, "5")
        connection.execute(sql_insert, 5)
        vals = connection.query(sql_select)
        assert_equal "5", vals[0][0]
        assert_equal "5", vals[1][0]
      elsif cols[i] == 'C10'
        connection.execute(sql_insert, "1234567890")
        connection.execute(sql_insert, 1234567890)
        vals = connection.query(sql_select)
        assert_equal "1234567890", vals[0][0]
        assert_equal "1234567890", vals[1][0]
      elsif cols[i] == 'DT'
        connection.execute(sql_insert, Date.civil(2000,2,2))
        connection.execute(sql_insert, "2000/2/2")
        connection.execute(sql_insert, "2000-2-2")
        vals = connection.query(sql_select)
        assert_equal Date.civil(2000,2,2), vals[0][0]
        assert_equal Date.civil(2000,2,2), vals[1][0]
      elsif cols[i] == 'TM'
        connection.execute(sql_insert, Time.utc(2000,1,1,2,22,22))
        connection.execute(sql_insert, "2000/1/1 2:22:22")
        connection.execute(sql_insert, "2000-1-1 2:22:22")
        vals = connection.query(sql_select)
        assert_equal Time.utc(2000,1,1,2,22,22), vals[0][0]
        assert_equal Time.utc(2000,1,1,2,22,22), vals[1][0]
      elsif cols[i] ==  'TS'
        connection.execute(sql_insert, Time.local(2006,6,6,3,33,33))
        connection.execute(sql_insert, "2006/6/6 3:33:33")
        connection.execute(sql_insert, "2006-6-6 3:33:33")
        vals = connection.query(sql_select)
        assert_equal Time.local(2006,6,6,3,33,33), vals[0][0]
        assert_equal Time.local(2006,6,6,3,33,33), vals[1][0]
        assert_equal Time.local(2006,6,6,3,33,33), vals[2][0]
      elsif cols[i] == 'N92'
        connection.execute(sql_insert, 12345.12)
        connection.execute(sql_insert, "12345.12")
        vals = connection.query(sql_select)
        # puts vals.inspect
        assert_equal 12345.12, vals[0][0], "NUMERIC (decimal)"
        assert_equal 12345.12, vals[1][0], "NUMERIC (string)"
      elsif cols[i] == 'D92'
        connection.execute(sql_insert, 12345.12)
        connection.execute(sql_insert, "12345.12")
        vals = connection.query(sql_select)
        # puts vals.inspect
        assert_equal 12345.12, vals[0][0], "DECIMAL (decimal)"
        assert_equal 12345.12, vals[1][0], "DECIMAL (string)"
      elsif cols[i] == 'N154'
        connection.execute(sql_insert, 12345.12)
        connection.execute(sql_insert, "12345.12")
        vals = connection.query(sql_select)
        # puts vals.inspect
        assert_equal 12345.12, vals[0][0], "NUMERIC (decimal)"
        assert_equal 12345.12, vals[1][0], "NUMERIC (string)"
      end
    end
    connection.drop
  end
end
*/
