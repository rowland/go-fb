package fb

import (
	"errors"
	"fmt"
	"strconv"
)

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
	case fmt.Stringer:
		s = v.String()
	default:
		return "", errors.New("string value expected")
	}
	return
}
