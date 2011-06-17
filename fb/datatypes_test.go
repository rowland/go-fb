package fb

import (
	"testing"
	"fmt"
	"strings"
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
	return time.Time{Year: 2000, Month: i + 1, Day: i + 1}
}

func genTm(i int) time.Time {
	return time.Time{Year: 1990, Month: 1, Day: 1, Hour: 12, Minute: i, Second: i}
}

func genTs(i int) time.Time {
	return time.Time{Year: 2006, Month: 1, Day: 1, Hour: i, Minute: i, Second: i}
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

func TestInsertCorrectTypes(t *testing.T) {
	cols := []string{"I", "SI", "BI", "F", "D", "C", "C10", "VC", "VC10", "VC10000", "DT", "TM", "TS", "N92", "D92", "N154"}
	types := []string{"INTEGER", "SMALLINT", "BIGINT", "FLOAT", "DOUBLE PRECISION", "CHAR", "CHAR(10)", "VARCHAR(1)", "VARCHAR(10)", "VARCHAR(10000)", "DATE", "TIME", "TIMESTAMP", "NUMERIC(9,2)", "DECIMAL(9,2)", "NUMERIC(15,4)"}

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	for i, col := range cols {
		sqlSchema := fmt.Sprintf("CREATE TABLE TEST_%s (VAL %s);", cols[i], types[i])
		sqlInsert := fmt.Sprintf("INSERT INTO TEST_%s (VAL) VALUES (?);", col)
		sqlSelect := fmt.Sprintf("SELECT * FROM TEST_%s;", col)
		var cursor *Cursor
		var err os.Error

		switch col {
		case "I":
			fmt.Println(sqlSchema)
			if _, err = conn.Execute(sqlSchema); err != nil {
				t.Fatalf("Error executing schema: %s", err)
			}
			conn.Commit()

			fmt.Println(sqlInsert)
			if _, err = conn.Execute(sqlInsert, 500000); err != nil {
				t.Fatalf("Error executing insert (1): %s", err)
			}
			fmt.Println(sqlInsert)
			if _, err = conn.Execute(sqlInsert, "500000"); err != nil {
				t.Fatalf("Error executing insert (2): %s", err)
			}
			var vals []interface{}
			if cursor, err = conn.Execute(sqlSelect); err != nil {
				t.Errorf("Unexpected error in select: %s", err)
				break
			}
			if err = cursor.Fetch(&vals); err != nil {
				t.Errorf("Error in fetch: %s", err)
				break
			}
			if vals[0].(int32) != 500000 {
				t.Errorf("Expected %d, got %d", 500000, vals[0])
			}
		}
	}
}

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
