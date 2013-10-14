package fb

import (
	"testing"
	"time"
)

func Test_bytesFromIf(t *testing.T) {
	var bs []byte = []byte("bytesFromIf test")
	if v, err := bytesFromIf(bs); err != nil || string(v) != "bytesFromIf test" {
		t.Errorf("bytesFromIf from []byte failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&bs); err != nil || string(v) != "bytesFromIf test" {
		t.Errorf("bytesFromIf from []byte failed: got %v, %v", v, err)
	}

	var f64 float64 = 123.456
	if v, err := bytesFromIf(f64); err != nil || string(v) != "123.456" {
		t.Errorf("bytesFromIf from float64 failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&f64); err != nil || string(v) != "123.456" {
		t.Errorf("bytesFromIf from float64 failed: got %v, %v", v, err)
	}

	var f32 float32 = 123.5
	if v, err := bytesFromIf(f32); err != nil || string(v) != "123.5" {
		t.Errorf("bytesFromIf from float32 failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&f32); err != nil || string(v) != "123.5" {
		t.Errorf("bytesFromIf from float32 failed: got %v, %v", v, err)
	}

	var i64 int64 = 123456
	if v, err := bytesFromIf(i64); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&i64); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int64 failed: got %v, %v", v, err)
	}

	var i32 int32 = 123456
	if v, err := bytesFromIf(i32); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&i32); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := bytesFromIf(i); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&i); err != nil || string(v) != "123456" {
		t.Errorf("bytesFromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123.456"
	if v, err := bytesFromIf(s); err != nil || string(v) != "123.456" {
		t.Errorf("bytesFromIf from string failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&s); err != nil || string(v) != "123.456" {
		t.Errorf("bytesFromIf from string failed: got %v, %v", v, err)
	}

	var b bool = true
	if v, err := bytesFromIf(b); err != nil || string(v) != "true" {
		t.Errorf("bytesFromIf from bool failed: got %v, %v", v, err)
	}
	if v, err := bytesFromIf(&b); err != nil || string(v) != "true" {
		t.Errorf("bytesFromIf from bool failed: got %v, %v", v, err)
	}
}

func Test_float64FromIf(t *testing.T) {
	var f64 float64 = 123.456
	if v, err := float64FromIf(f64); err != nil || v != 123.456 {
		t.Errorf("float64FromIf from float64 failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&f64); err != nil || v != 123.456 {
		t.Errorf("float64FromIf from float64 failed: got %v, %v", v, err)
	}

	var f32 float32 = 123.5
	if v, err := float64FromIf(f32); err != nil || v != 123.5 {
		t.Errorf("float64FromIf from float32 failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&f32); err != nil || v != 123.5 {
		t.Errorf("float64FromIf from float32 failed: got %v, %v", v, err)
	}

	var i64 int64 = 123456
	if v, err := float64FromIf(i64); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&i64); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int64 failed: got %v, %v", v, err)
	}

	var i32 int32 = 123456
	if v, err := float64FromIf(i32); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&i32); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := float64FromIf(i); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&i); err != nil || v != 123456 {
		t.Errorf("float64FromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123.456"
	if v, err := float64FromIf(s); err != nil || v != 123.456 {
		t.Errorf("float64FromIf from string failed: got %v, %v", v, err)
	}
	if v, err := float64FromIf(&s); err != nil || v != 123.456 {
		t.Errorf("float64FromIf from string failed: got %v, %v", v, err)
	}
}

func Test_float32FromIf(t *testing.T) {
	var f64 float64 = 123.456
	if v, err := float32FromIf(f64); err != nil || v != 123.456 {
		t.Errorf("float32FromIf from float64 failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&f64); err != nil || v != 123.456 {
		t.Errorf("float32FromIf from float64 failed: got %v, %v", v, err)
	}

	var f32 float32 = 123.5
	if v, err := float32FromIf(f32); err != nil || v != 123.5 {
		t.Errorf("float32FromIf from float32 failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&f32); err != nil || v != 123.5 {
		t.Errorf("float32FromIf from float32 failed: got %v, %v", v, err)
	}

	var i64 int64 = 123456
	if v, err := float32FromIf(i64); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&i64); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int64 failed: got %v, %v", v, err)
	}

	var i32 int32 = 123456
	if v, err := float32FromIf(i32); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&i32); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := float32FromIf(i); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&i); err != nil || v != 123456 {
		t.Errorf("float32FromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123.456"
	if v, err := float32FromIf(s); err != nil || v != 123.456 {
		t.Errorf("float32FromIf from string failed: got %v, %v", v, err)
	}
	if v, err := float32FromIf(&s); err != nil || v != 123.456 {
		t.Errorf("float32FromIf from string failed: got %v, %v", v, err)
	}
}

func Test_int64FromIf(t *testing.T) {
	var i64 int64 = 123456
	if v, err := int64FromIf(i64); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := int64FromIf(&i64); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int64 failed: got %v, %v", v, err)
	}

	var i32 int32 = 123456
	if v, err := int64FromIf(i32); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := int64FromIf(&i32); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := int64FromIf(i); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int failed: got %v, %v", v, err)
	}
	if v, err := int64FromIf(&i); err != nil || v != 123456 {
		t.Errorf("int64FromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123456"
	if v, err := int64FromIf(s); err != nil || v != 123456 {
		t.Errorf("int64FromIf from string failed: got %v, %v", v, err)
	}
	if v, err := int64FromIf(&s); err != nil || v != 123456 {
		t.Errorf("int64FromIf from string failed: got %v, %v", v, err)
	}
}

func Test_int32FromIf(t *testing.T) {
	var i64 int64 = 123456
	if v, err := int32FromIf(i64); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := int32FromIf(&i64); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int64 failed: got %v, %v", v, err)
	}

	var i64over int64 = int32max + 1
	if _, err := int32FromIf(i64over); err == nil {
		t.Errorf("int32FromIf from int64 should fail")
	}
	if _, err := int32FromIf(&i64over); err == nil {
		t.Errorf("int32FromIf from int64 should fail")
	}

	var i64under int64 = int32min - 1
	if _, err := int32FromIf(i64under); err == nil {
		t.Errorf("int32FromIf from int64 should fail")
	}
	if _, err := int32FromIf(&i64under); err == nil {
		t.Errorf("int32FromIf from int64 should fail")
	}

	var i32 int32 = 123456
	if v, err := int32FromIf(i32); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := int32FromIf(&i32); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := int32FromIf(i); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int failed: got %v, %v", v, err)
	}
	if v, err := int32FromIf(&i); err != nil || v != 123456 {
		t.Errorf("int32FromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123456"
	if v, err := int32FromIf(s); err != nil || v != 123456 {
		t.Errorf("int32FromIf from string failed: got %v, %v", v, err)
	}
	if v, err := int32FromIf(&s); err != nil || v != 123456 {
		t.Errorf("int32FromIf from string failed: got %v, %v", v, err)
	}
}

func Test_stringFromIf(t *testing.T) {
	var f64 float64 = 123.456
	if v, err := stringFromIf(f64); err != nil || v != "123.456" {
		t.Errorf("stringFromIf from float64 failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&f64); err != nil || v != "123.456" {
		t.Errorf("stringFromIf from float64 failed: got %v, %v", v, err)
	}

	var f32 float32 = 123.5
	if v, err := stringFromIf(f32); err != nil || v != "123.5" {
		t.Errorf("stringFromIf from float32 failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&f32); err != nil || v != "123.5" {
		t.Errorf("stringFromIf from float32 failed: got %v, %v", v, err)
	}

	var i64 int64 = 123456
	if v, err := stringFromIf(i64); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int64 failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&i64); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int64 failed: got %v, %v", v, err)
	}

	var i32 int32 = 123456
	if v, err := stringFromIf(i32); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int32 failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&i32); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int32 failed: got %v, %v", v, err)
	}

	var i int = 123456
	if v, err := stringFromIf(i); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&i); err != nil || v != "123456" {
		t.Errorf("stringFromIf from int failed: got %v, %v", v, err)
	}

	var s string = "123.456"
	if v, err := stringFromIf(s); err != nil || v != "123.456" {
		t.Errorf("stringFromIf from string failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&s); err != nil || v != "123.456" {
		t.Errorf("stringFromIf from string failed: got %v, %v", v, err)
	}

	var b bool = true
	if v, err := stringFromIf(b); err != nil || v != "true" {
		t.Errorf("stringFromIf from bool failed: got %v, %v", v, err)
	}
	if v, err := stringFromIf(&b); err != nil || v != "true" {
		t.Errorf("stringFromIf from bool failed: got %v, %v", v, err)
	}
}

func Test_timeFromIf(t *testing.T) {
	dt := time.Date(2006, 6, 6, 3, 33, 33, 0, time.Local)
	if v, err := timeFromIf(dt, time.Local); err != nil || !v.Equal(dt) {
		t.Errorf("timeFromIf from Time failed: got %v, expected %v, %v", v, dt, err)
	}

	dt2 := "2006/6/6 3:33:33"
	if v, err := timeFromIf(dt2, time.Local); err != nil || !v.Equal(dt) {
		t.Errorf("timeFromIf from string with slashes failed: got %v, expected %v, %v", v, dt, err)
	}

	dt3 := "2006-6-6 3:33:33"
	if v, err := timeFromIf(dt3, time.Local); err != nil || !v.Equal(dt) {
		t.Errorf("timeFromIf from string with dashes failed: got %v, expected %v, %v", v, dt, err)
	}

	dtUTC := time.Date(2006, 6, 6, 3, 33, 33, 0, time.UTC)
	dt4 := "2006-6-6 3:33:33"
	loc, _ := time.LoadLocation("")
	if v, err := timeFromIf(dt4, loc); err != nil || !v.Equal(dtUTC) {
		t.Errorf("timeFromIf from string with dashes failed: got %v, expected %v, %v", v, dtUTC, err)
	}
}

func Test_timeReality(t *testing.T) {
	loc, _ := time.LoadLocation("US/Arizona")
	t1, _ := time.ParseInLocation(timeWithDashes, "2006-1-2 15:04:05", loc)
	u := t1.Unix()
	n := t1.Nanosecond()
	t2 := time.Unix(u, int64(n))
	if !t1.Equal(t2) {
		t.Errorf("Unix times are incompatible: got %v, expected %v", t2, t1)
	}
}

func TestConvertValue_bool(t *testing.T) {
	var b bool
	var err error

	b = false
	if err = ConvertValue(&b, 1); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from 1")
	}

	b = false
	if err = ConvertValue(&b, "t"); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from \"t\"")
	}

	b = false
	if err = ConvertValue(&b, "T"); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from \"T\"")
	}

	b = false
	if err = ConvertValue(&b, "TRUE"); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from \"TRUE\"")
	}

	b = false
	if err = ConvertValue(&b, "true"); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from \"true\"")
	}

	b = false
	if err = ConvertValue(&b, "True"); err != nil {
		t.Error(err)
	}
	if !b {
		t.Error("bool failed conversion from \"True\"")
	}

	b = true
	if err = ConvertValue(&b, 0); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from 0")
	}

	b = true
	if err = ConvertValue(&b, "f"); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from \"f\"")
	}

	b = true
	if err = ConvertValue(&b, "F"); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from \"F\"")
	}

	b = true
	if err = ConvertValue(&b, "FALSE"); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from \"FALSE\"")
	}

	b = true
	if err = ConvertValue(&b, "false"); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from \"false\"")
	}

	b = true
	if err = ConvertValue(&b, "False"); err != nil {
		t.Error(err)
	}
	if b {
		t.Error("bool failed conversion from \"False\"")
	}

	var nb NullableBool
	if err = ConvertValue(&nb, 1); err != nil {
		t.Error(err)
	}
	if nb.Null {
		t.Error("nb should not be null")
	}
	if !nb.Value {
		t.Error("nb should be true")
	}

	var nb2 NullableBool
	if err = ConvertValue(&nb2, nil); err != nil {
		t.Error(err)
	}
	if !nb2.Null {
		t.Error("nb should be null")
	}
	if nb2.Value {
		t.Error("nb should be false")
	}
}

func TestConvertValue_bytes(t *testing.T) {
	var bs []byte = []byte("TestConvertValue_bytes test")
	var bytes []byte
	var err error

	if err = ConvertValue(&bytes, bs); err != nil {
		t.Error(err)
	}
	if string(bytes) != "TestConvertValue_bytes test" {
		t.Errorf("TestConvertValue_bytes failed: expected %v, got %v", bs, bytes)
	}
}

func TestConvertValue_int(t *testing.T) {
	var i int
	var err error

	if err = ConvertValue(&i, 123); err != nil {
		t.Error(err)
	}
	if i != 123 {
		t.Errorf("Expected 123, got %v", i)
	}
}

func TestConvertValue_int32(t *testing.T) {
	var i int32
	var err error

	if err = ConvertValue(&i, 123); err != nil {
		t.Error(err)
	}
	if i != 123 {
		t.Errorf("Expected 123, got %v", i)
	}

	var ni NullableInt32
	if err = ConvertValue(&ni, int32(123)); err != nil {
		t.Error(err)
	}
	if ni.Null {
		t.Error("ni should not be null")
	}
	if ni.Value != 123 {
		t.Errorf("Expected 123, got %v", ni.Value)
	}

	var ni2 NullableInt32
	if err = ConvertValue(&ni2, nil); err != nil {
		t.Error(err)
	}
	if !ni2.Null {
		t.Error("ni2 should be null")
	}
}

func TestConvertValue_int64(t *testing.T) {
	var i int64
	var err error

	if err = ConvertValue(&i, 123); err != nil {
		t.Error(err)
	}
	if i != 123 {
		t.Errorf("Expected 123, got %v", i)
	}

	var ni NullableInt64
	if err = ConvertValue(&ni, int64(123)); err != nil {
		t.Error(err)
	}
	if ni.Null {
		t.Error("ni should not be null")
	}
	if ni.Value != 123 {
		t.Errorf("Expected 123, got %v", ni.Value)
	}

	var ni2 NullableInt64
	if err = ConvertValue(&ni2, nil); err != nil {
		t.Error(err)
	}
	if !ni2.Null {
		t.Error("ni2 should be null")
	}
}

func TestConvertValue_interface(t *testing.T) {
	var i interface{}
	var err error

	if err = ConvertValue(&i, 123); err != nil {
		t.Error(err)
	}
	if i.(int) != 123 {
		t.Errorf("Expected 123, got %v", i)
	}
}

func TestConvertValue_float32(t *testing.T) {
	var f float32
	var err error

	if err = ConvertValue(&f, 123.24); err != nil {
		t.Error(err)
	}
	if f != 123.24 {
		t.Errorf("Expected 123.24, got %v", f)
	}

	var nf NullableFloat32
	if err = ConvertValue(&nf, float32(123.24)); err != nil {
		t.Error(err)
	}
	if nf.Null {
		t.Error("nf should not be null")
	}
	if nf.Value != 123.24 {
		t.Errorf("Expected 123.24, got %v", nf.Value)
	}

	var nf2 NullableFloat32
	if err = ConvertValue(&nf2, nil); err != nil {
		t.Error(err)
	}
	if !nf2.Null {
		t.Error("nf2 should be null")
	}
}

func TestConvertValue_float64(t *testing.T) {
	var f float64
	var err error

	if err = ConvertValue(&f, 123.24); err != nil {
		t.Error(err)
	}
	if f != 123.24 {
		t.Errorf("Expected 123.24, got %v", f)
	}

	var nf NullableFloat64
	if err = ConvertValue(&nf, float64(123.24)); err != nil {
		t.Error(err)
	}
	if nf.Null {
		t.Error("nf should not be null")
	}
	if nf.Value != 123.24 {
		t.Errorf("Expected 123.24, got %v", nf.Value)
	}

	var nf2 NullableFloat64
	if err = ConvertValue(&nf2, nil); err != nil {
		t.Error(err)
	}
	if !nf2.Null {
		t.Error("nf2 should be null")
	}
}

func TestConvertValue_string(t *testing.T) {
	var s string
	var err error

	if err = ConvertValue(&s, "TestConvertValue_string test"); err != nil {
		t.Error(err)
	}
	if s != "TestConvertValue_string test" {
		t.Errorf("Expected \"TestConvertValue_string test\", got \"%v\"", s)
	}

	var ns NullableString
	if err = ConvertValue(&ns, "Foo Bar"); err != nil {
		t.Error(err)
	}
	if ns.Value != "Foo Bar" {
		t.Errorf("Expected \"Foo Bar\", got %v", ns.Value)
	}
	if ns.Null {
		t.Error("ns should not be null")
	}

	var ns2 NullableString
	if err = ConvertValue(&ns2, nil); err != nil {
		t.Error(err)
	}
	if !ns2.Null {
		t.Error("ns2 should be null")
	}
	if ns2.Value != "" {
		t.Error("ns2 should be \"\"")
	}
}

func TestConvertValue_time(t *testing.T) {
	var tm time.Time
	var err error

	dt := time.Date(2013, 10, 9, 20, 31, 0, 0, time.Local)
	if err = ConvertValue(&tm, dt); err != nil {
		t.Error(err)
	}
	if tm != dt {
		t.Errorf("Expected %v, got %v", tm)
	}

	var tm2 time.Time
	if err = ConvertValue(&tm2, "2013-10-9 20:31:00.000"); err != nil {
		t.Error(err)
	}
	if tm2 != dt {
		t.Errorf("Expected %v, got %v", tm)
	}
	
	var nt NullableTime
	if err = ConvertValue(&nt, dt); err != nil {
		t.Error(err)
	}
	if nt.Value != dt {
		t.Errorf("Expected %v, got %v", tm)
	}
	if nt.Null {
		t.Error("nt should not be null")
	}
	var nt2 NullableTime
	if err = ConvertValue(&nt2, nil); err != nil {
		t.Error(err)
	}
	if !nt2.Null {
		t.Error("nt2 should be null")
	}
	ztm := time.Time{}
	if nt2.Value != ztm {
		t.Error("nt2 should be Time zero value")
	}
}
