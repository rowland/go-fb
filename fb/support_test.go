package fb

import (
	"testing"
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
