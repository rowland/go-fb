package fb

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

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

const (
	// Mon Jan 2 15:04:05 -0700 MST 2006
	timeWithSlashes = "2006/1/2 15:04:05"
	timeWithDashes  = "2006-1-2 15:04:05"
)

func parseUnknownTime(s string, location *time.Location) (t time.Time, err error) {
	if t, err = time.ParseInLocation(timeWithSlashes, s, location); err == nil {
		return
	}
	if t, err = time.ParseInLocation(timeWithDashes, s, location); err == nil {
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
