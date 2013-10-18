package fb

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode"
)

const TestConnectionStringLowerNames = TestConnectionString + "lowercase_names=true;"

func TestExecute(t *testing.T) {
	const SqlSchema = "CREATE TABLE TEST (ID INT, NAME VARCHAR(20))"
	const SqlSelect = "SELECT * FROM RDB$DATABASE"

	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if conn.TransactionStarted() {
		t.Fatal("Transaction should not be started before a statement is executed.")
	}
	if _, err := conn.Execute(SqlSchema); err != nil {
		t.Fatalf("Unexpected error executing schema statment: %s", err)
	}
	if _, err := conn.Execute(SqlSelect); err != nil {
		t.Fatalf("Unexpected error executing select statment: %s", err)
	}
	if !conn.TransactionStarted() {
		t.Error("Transaction should be started")
	}
	if err := conn.Commit(); err != nil {
		t.Fatalf("Unexpected error committing transaction: %s", err)
	}
	if conn.TransactionStarted() {
		t.Fatal("Transaction should not be started after transaction is committed.")
	}
}

func TestTableNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = "CREATE TABLE TEST1 (ID INTEGER); CREATE TABLE TEST2 (ID INTEGER);"
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tableNames, err := conn.TableNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(tableNames))
	st.Equal("TEST1", tableNames[0])
	st.Equal("TEST2", tableNames[1])
}

func TestTableNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = "CREATE TABLE TEST1 (ID INTEGER); CREATE TABLE TEST2 (ID INTEGER);"
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tableNames, err := conn.TableNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(tableNames))
	st.Equal("test1", tableNames[0])
	st.Equal("test2", tableNames[1])
}

func TestViewNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE TABLE TEST1 (ID INT, NAME1 VARCHAR(10));
		CREATE TABLE TEST2 (ID INT, NAME2 VARCHAR(10));
		CREATE VIEW VIEW1 AS SELECT TEST1.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.ID = TEST2.ID;
		CREATE VIEW VIEW2 AS SELECT TEST2.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.NAME1 = TEST2.NAME2;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	viewNames, err := conn.ViewNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(viewNames))
	st.Equal("VIEW1", viewNames[0])
	st.Equal("VIEW2", viewNames[1])
}

func TestViewNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE TABLE TEST1 (ID INT, NAME1 VARCHAR(10));
		CREATE TABLE TEST2 (ID INT, NAME2 VARCHAR(10));
		CREATE VIEW VIEW1 AS SELECT TEST1.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.ID = TEST2.ID;
		CREATE VIEW VIEW2 AS SELECT TEST2.ID, TEST1.NAME1, TEST2.NAME2 FROM TEST1 JOIN TEST2 ON TEST1.NAME1 = TEST2.NAME2;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	viewNames, err := conn.ViewNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(viewNames))
	st.Equal("view1", viewNames[0])
	st.Equal("view2", viewNames[1])
}

func TestGeneratorNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE GENERATOR TEST1_SEQ;
		CREATE GENERATOR TEST2_SEQ;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	genNames, err := conn.GeneratorNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(genNames))
	st.Equal("TEST1_SEQ", genNames[0])
	st.Equal("TEST2_SEQ", genNames[1])
}

func TestGeneratorNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE GENERATOR TEST1_SEQ;
		CREATE GENERATOR TEST2_SEQ;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	genNames, err := conn.GeneratorNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(genNames))
	st.Equal("test1_seq", genNames[0])
	st.Equal("test2_seq", genNames[1])
}

func TestRoleNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		create role reader;
		create role writer;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	roleNames, err := conn.RoleNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(roleNames))
	st.Equal("READER", roleNames[0])
	st.Equal("WRITER", roleNames[1])
}

func TestRoleNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		create role reader;
		create role writer;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	roleNames, err := conn.RoleNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(2, len(roleNames))
	st.Equal("reader", roleNames[0])
	st.Equal("writer", roleNames[1])
}

func TestProcedureNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE PROCEDURE PLUSONE(NUM1 INTEGER) RETURNS (NUM2 INTEGER) AS
		BEGIN
		  NUM2 = NUM1 + 1;
		  SUSPEND;
		END;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	procNames, err := conn.ProcedureNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(1, len(procNames))
	st.Equal("PLUSONE", procNames[0])
}

func TestProcedureNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE PROCEDURE PLUSONE(NUM1 INTEGER) RETURNS (NUM2 INTEGER) AS
		BEGIN
		  NUM2 = NUM1 + 1;
		  SUSPEND;
		END;
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if _, err = conn.Execute(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	procNames, err := conn.ProcedureNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(1, len(procNames))
	st.Equal("plusone", procNames[0])
}

func TestTriggerNames(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE TABLE TEST (ID INT, NAME VARCHAR(20));
		CREATE GENERATOR TEST_SEQ;
	`
	const triggerSchema = `
		CREATE TRIGGER TEST_INSERT FOR TEST ACTIVE BEFORE INSERT AS
		BEGIN
			IF (NEW.ID IS NULL) THEN
				NEW.ID = CAST(GEN_ID(TEST_SEQ, 1) AS INT);
		END
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err = conn.Execute(triggerSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	triggerNames, err := conn.TriggerNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(1, len(triggerNames))
	st.Equal("TEST_INSERT", triggerNames[0])
}

func TestTriggerNamesLower(t *testing.T) {
	st := SuperTest{t}
	const sqlSchema = `
		CREATE TABLE TEST (ID INT, NAME VARCHAR(20));
		CREATE GENERATOR TEST_SEQ;
	`
	const triggerSchema = `
		CREATE TRIGGER TEST_INSERT FOR TEST ACTIVE BEFORE INSERT AS
		BEGIN
			IF (NEW.ID IS NULL) THEN
				NEW.ID = CAST(GEN_ID(TEST_SEQ, 1) AS INT);
		END
	`
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionStringLowerNames)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err = conn.Execute(triggerSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	triggerNames, err := conn.TriggerNames()
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(1, len(triggerNames))
	st.Equal("test_insert", triggerNames[0])
}

var expectedColumns = []Column{
	{Name: "ID", Domain: "", SqlType: "BIGINT", SqlSubtype: NullableInt16{0, false}, Length: 8, Precision: NullableInt16{0, false}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{true, false}},
	{Name: "FLAG", Domain: "BOOLEAN", SqlType: "INTEGER", SqlSubtype: NullableInt16{0, false}, Length: 4, Precision: NullableInt16{0, false}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "BINARY", Domain: "", SqlType: "BLOB", SqlSubtype: NullableInt16{0, false}, Length: 8, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "I", Domain: "", SqlType: "INTEGER", SqlSubtype: NullableInt16{0, false}, Length: 4, Precision: NullableInt16{0, false}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "I32", Domain: "", SqlType: "INTEGER", SqlSubtype: NullableInt16{0, false}, Length: 4, Precision: NullableInt16{0, false}, Scale: 0, Default: NullableString{"0", false}, Nullable: NullableBool{false, true}},
	{Name: "I64", Domain: "", SqlType: "BIGINT", SqlSubtype: NullableInt16{0, false}, Length: 8, Precision: NullableInt16{0, false}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "F32", Domain: "", SqlType: "FLOAT", SqlSubtype: NullableInt16{0, true}, Length: 4, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "F64", Domain: "", SqlType: "DOUBLE PRECISION", SqlSubtype: NullableInt16{0, true}, Length: 8, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"0.0", false}, Nullable: NullableBool{false, true}},
	{Name: "C", Domain: "", SqlType: "CHAR", SqlSubtype: NullableInt16{0, false}, Length: 1, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "CS", Domain: "ALPHABET", SqlType: "CHAR", SqlSubtype: NullableInt16{0, false}, Length: 26, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "V", Domain: "", SqlType: "VARCHAR", SqlSubtype: NullableInt16{0, false}, Length: 1, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "VS", Domain: "ALPHA", SqlType: "VARCHAR", SqlSubtype: NullableInt16{0, false}, Length: 26, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "M", Domain: "", SqlType: "BLOB", SqlSubtype: NullableInt16{1, false}, Length: 8, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "DT", Domain: "", SqlType: "DATE", SqlSubtype: NullableInt16{0, true}, Length: 4, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "TM", Domain: "", SqlType: "TIME", SqlSubtype: NullableInt16{0, true}, Length: 4, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "TS", Domain: "", SqlType: "TIMESTAMP", SqlSubtype: NullableInt16{0, true}, Length: 8, Precision: NullableInt16{0, true}, Scale: 0, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "N92", Domain: "", SqlType: "NUMERIC", SqlSubtype: NullableInt16{1, false}, Length: 4, Precision: NullableInt16{9, false}, Scale: -2, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
	{Name: "D92", Domain: "", SqlType: "DECIMAL", SqlSubtype: NullableInt16{2, false}, Length: 4, Precision: NullableInt16{9, false}, Scale: -2, Default: NullableString{"", true}, Nullable: NullableBool{false, true}},
}

func TestColumns(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	if err = conn.ExecuteScript(sqlSampleSchema); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cols, err := conn.Columns("TEST")
	if err != nil {
		t.Fatal(err)
	}
	st.MustEqual(18, len(cols))
	for i, exp := range expectedColumns {
		if !reflect.DeepEqual(&exp, cols[i]) {
			t.Errorf("Expected %v, got %v", &exp, cols[i])
		}
	}
}

func TestQueryRows(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSelect := "SELECT * FROM TEST;"

	if err = conn.ExecuteScript(sqlSampleSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	for id := 0; id < 10; id++ {
		if _, err = conn.Execute(sqlSampleInsert,
			genBi(id),
			int(id%2),
			nil,
			genI(id),
			genI(id),
			genBi(id),
			genF(id),
			genD(id),
			genC(id),
			genC10(id),
			genVc(id),
			genVc10(id),
			genVc10000(id),
			genDt(id).In(conn.Location),
			genTm(id).In(conn.Location),
			genTs(id).In(conn.Location),
			genN92(id),
			genD92(id)); err != nil {
			t.Fatalf("Error executing insert: %s", err)
		}
	}

	var rows [][]interface{}
	if rows, err = conn.QueryRows(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}

	if len(rows) != 10 {
		t.Fatalf("Expected %d rows, got %d", 10, len(rows))
	}

	for id, row := range rows {
		st.Equal(genBi(id), row[0])
		st.Equal(int32(id%2), row[1])
		st.Equal(nil, row[2])
		st.Equal(genI(id), row[3])
		st.Equal(genI(id), row[4])
		st.Equal(genBi(id), row[5])
		st.Equal(genF(id), row[6])
		st.Equal(genD(id), row[7])
		st.Equal(genC(id), row[8])
		st.Equal(genC10(id), strings.TrimRightFunc(row[9].(string), unicode.IsSpace))
		st.Equal(genVc(id), row[10])
		st.Equal(genVc10(id), row[11])
		st.Equal(genVc10000(id), row[12])
		st.Equal(genDt(id).In(conn.Location), row[13])
		st.Equal(genTm(id).In(conn.Location), row[14])
		st.Equal(genTs(id).In(conn.Location), row[15])
		st.Equal(genN92(id), row[16])
		st.Equal(genD92(id), row[17])
	}
}

func TestQueryRowMaps(t *testing.T) {
	st := SuperTest{t}
	os.Remove(TestFilename)

	conn, err := Create(TestConnectionString)
	if err != nil {
		t.Fatalf("Unexpected error creating database: %s", err)
	}
	defer conn.Drop()

	sqlSelect := "SELECT * FROM TEST;"

	if err = conn.ExecuteScript(sqlSampleSchema); err != nil {
		t.Fatalf("Error executing schema: %s", err)
	}

	for id := 0; id < 10; id++ {
		if _, err = conn.Execute(sqlSampleInsert,
			genBi(id),
			int(id%2),
			nil,
			genI(id),
			genI(id),
			genBi(id),
			genF(id),
			genD(id),
			genC(id),
			genC10(id),
			genVc(id),
			genVc10(id),
			genVc10000(id),
			genDt(id).In(conn.Location),
			genTm(id).In(conn.Location),
			genTs(id).In(conn.Location),
			genN92(id),
			genD92(id)); err != nil {
			t.Fatalf("Error executing insert: %s", err)
		}
	}

	var rows []map[string]interface{}
	if rows, err = conn.QueryRowMaps(sqlSelect); err != nil {
		t.Fatalf("Unexpected error in select: %s", err)
	}

	if len(rows) != 10 {
		t.Fatalf("Expected %d rows, got %d", 10, len(rows))
	}

	for id, row := range rows {
		st.Equal(genBi(id), row["ID"])
		st.Equal(int32(id%2), row["FLAG"])
		st.Equal(nil, row["BINARY"])
		st.Equal(genI(id), row["I"])
		st.Equal(genI(id), row["I32"])
		st.Equal(genBi(id), row["I64"])
		st.Equal(genF(id), row["F32"])
		st.Equal(genD(id), row["F64"])
		st.Equal(genC(id), row["C"])
		st.Equal(genC10(id), strings.TrimRightFunc(row["CS"].(string), unicode.IsSpace))
		st.Equal(genVc(id), row["V"])
		st.Equal(genVc10(id), row["VS"])
		st.Equal(genVc10000(id), row["M"])
		st.Equal(genDt(id).In(conn.Location), row["DT"])
		st.Equal(genTm(id).In(conn.Location), row["TM"])
		st.Equal(genTs(id).In(conn.Location), row["TS"])
		st.Equal(genN92(id), row["N92"])
		st.Equal(genD92(id), row["D92"])
	}
}
