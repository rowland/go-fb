package fb

import (
	"testing"
	"time"
)

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
	// if t1.String() != t2.String() {
	// 	t.Errorf("Unix times are incompatible: got %v, expected %v", t2, t1)
	// }
}
