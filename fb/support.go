package fb

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

const (
	int16max  = (1 << 15) - 1
	int16min  = -(1 << 15)
	uint16max = (1 << 16) - 1
	int32max  = (1 << 31) - 1
	int32min  = -(1 << 31)
	uint32max = (1 << 32) - 1
)

var reLowercase = regexp.MustCompile("[a-z]")

func boolFromIf(v interface{}) (b bool, err error) {
	var s string
	s, err = stringFromIf(v)
	if err == nil {
		b, err = strconv.ParseBool(s)
	}
	return
}

func bytesFromIf(v interface{}) (b []byte, err error) {
	switch v := v.(type) {
	case []byte:
		b = v
	case *[]byte:
		b = *v
	case string:
		b = []byte(v)
	case *string:
		b = []byte(*v)
	case int64:
		b = strconv.AppendInt(b, v, 10)
	case *int64:
		b = strconv.AppendInt(b, *v, 10)
	case int32:
		b = strconv.AppendInt(b, int64(v), 10)
	case *int32:
		b = strconv.AppendInt(b, int64(*v), 10)
	case int:
		b = strconv.AppendInt(b, int64(v), 10)
	case *int:
		b = strconv.AppendInt(b, int64(*v), 10)
	case float64:
		b = strconv.AppendFloat(b, v, 'f', -1, 64)
	case *float64:
		b = strconv.AppendFloat(b, *v, 'f', -1, 64)
	case float32:
		b = strconv.AppendFloat(b, float64(v), 'f', -1, 64)
	case *float32:
		b = strconv.AppendFloat(b, float64(*v), 'f', -1, 64)
	case bool:
		b = strconv.AppendBool(b, v)
	case *bool:
		b = strconv.AppendBool(b, *v)
	case fmt.Stringer:
		b = []byte(v.String())
	default:
		return b, errors.New("[]byte value expected")
	}
	return
}

func float64FromIf(v interface{}) (f float64, err error) {
	switch d := v.(type) {
	case float64:
		f = d
	case *float64:
		f = *d
	case float32:
		f = float64(d)
	case *float32:
		f = float64(*d)
	case int64:
		f = float64(d)
	case *int64:
		f = float64(*d)
	case int32:
		f = float64(d)
	case *int32:
		f = float64(*d)
	case int:
		f = float64(d)
	case *int:
		f = float64(*d)
	case string:
		f, err = strconv.ParseFloat(d, 64)
	case *string:
		f, err = strconv.ParseFloat(*d, 64)
	default:
		return 0.0, errors.New("numeric value expected")
	}
	if err != nil {
		return 0.0, err
	}
	return
}

func float32FromIf(v interface{}) (f float32, err error) {
	var f64 float64
	f64, err = float64FromIf(v)
	f = float32(f64)
	return
}

func hasLowercase(s string) bool {
	return reLowercase.MatchString(s)
}

func int64FromIf(v interface{}) (i int64, err error) {
	switch v := v.(type) {
	case int64:
		i = v
	case *int64:
		i = *v
	case int32:
		i = int64(v)
	case *int32:
		i = int64(*v)
	case int16:
		i = int64(v)
	case *int16:
		i = int64(*v)
	case int:
		i = int64(v)
	case *int:
		i = int64(*v)
	case string:
		i, err = strconv.ParseInt(v, 10, 64)
	case *string:
		i, err = strconv.ParseInt(*v, 10, 64)
	case fmt.Stringer:
		i, err = strconv.ParseInt(v.String(), 10, 64)
	default:
		return 0, errors.New("integer value expected")
	}
	if err != nil {
		return 0, errors.New("integer value expected")
	}
	return
}

func int32FromIf(v interface{}) (i int32, err error) {
	var i64 int64
	if i64, err = int64FromIf(v); err != nil {
		return
	}
	if i64 > int32max || i64 < int32min {
		err = fmt.Errorf("Value %d is out of range %d - %d", i64, int32min, int32max)
	} else {
		i = int32(i64)
	}
	return
}

func int16FromIf(v interface{}) (i int16, err error) {
	var i64 int64
	if i64, err = int64FromIf(v); err != nil {
		return
	}
	if i64 > int16max || i64 < int16min {
		err = fmt.Errorf("Value %d is out of range %d - %d", i64, int16min, int16max)
	} else {
		i = int16(i64)
	}
	return
}

func intFromIf(v interface{}) (i int, err error) {
	var i32 int32
	i32, err = int32FromIf(v)
	i = int(i32)
	return
}

const (
	// Mon Jan 2 15:04:05 -0700 MST 2006
	dateWithDashes  = "2006-1-2"
	dateWithSlashes = "2006/1/2"
	timeWithDashes  = "2006-1-2 15:04:05"
	timeWithSlashes = "2006/1/2 15:04:05"
)

func parseUnknownTime(s string, location *time.Location) (t time.Time, err error) {
	if t, err = time.ParseInLocation(timeWithSlashes, s, location); err == nil {
		return
	}
	if t, err = time.ParseInLocation(timeWithDashes, s, location); err == nil {
		return
	}
	if t, err = time.ParseInLocation(dateWithSlashes, s, location); err == nil {
		return
	}
	if t, err = time.ParseInLocation(dateWithDashes, s, location); err == nil {
		return
	}
	if t, err = time.ParseInLocation(timeWithDashes, "1970-1-1 "+s, location); err == nil {
		return
	}
	return time.Time{}, err
}

func stringFromIf(v interface{}) (s string, err error) {
	switch v := v.(type) {
	case string:
		s = v
	case *string:
		s = *v
	case int64:
		s = strconv.FormatInt(v, 10)
	case *int64:
		s = strconv.FormatInt(*v, 10)
	case int32:
		s = strconv.FormatInt(int64(v), 10)
	case *int32:
		s = strconv.FormatInt(int64(*v), 10)
	case int16:
		s = strconv.FormatInt(int64(v), 10)
	case *int16:
		s = strconv.FormatInt(int64(*v), 10)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	case *int:
		s = strconv.FormatInt(int64(*v), 10)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case *float64:
		s = strconv.FormatFloat(*v, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case *float32:
		s = strconv.FormatFloat(float64(*v), 'f', -1, 64)
	case bool:
		s = strconv.FormatBool(v)
	case *bool:
		s = strconv.FormatBool(*v)
	case fmt.Stringer:
		s = v.String()
	default:
		return "", errors.New("string value expected")
	}
	return
}

func timeFromIf(v interface{}, location *time.Location) (t time.Time, err error) {
	switch v := v.(type) {
	case string:
		t, err = parseUnknownTime(v, location)
	case *string:
		t, err = parseUnknownTime(*v, location)
	case time.Time:
		t = v
	case *time.Time:
		t = *v
	case fmt.Stringer:
		t, err = parseUnknownTime(v.String(), location)
	default:
		return time.Time{}, errors.New("time value expected")
	}
	return
}

var errNilPointer = errors.New("ConvertValue: destination is nil")

func ConvertValue(dest, src interface{}) (err error) {
	if dest == nil {
		return errNilPointer
	}
	switch d := dest.(type) {
	case *bool:
		*d, err = boolFromIf(src)
	case *[]byte:
		*d, err = bytesFromIf(src)
	case *int:
		*d, err = intFromIf(src)
	case *int16:
		*d, err = int16FromIf(src)
	case *int32:
		*d, err = int32FromIf(src)
	case *int64:
		*d, err = int64FromIf(src)
	case *interface{}:
		*d = src
	case *float32:
		*d, err = float32FromIf(src)
	case *float64:
		*d, err = float64FromIf(src)
	case *string:
		*d, err = stringFromIf(src)
	case *time.Time:
		*d, err = timeFromIf(src, time.Local)
	case Scanner:
		err = d.Scan(src)
	}
	return
}
