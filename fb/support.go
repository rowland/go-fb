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
	case float32:
		f = float64(d)
	case int64:
		f = float64(d)
	case int32:
		f = float64(d)
	case int:
		f = float64(d)
	case string:
		f, err = strconv.ParseFloat(d, 64)
	}
	if err != nil {
		return 0.0, errors.New("numeric value expected")
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
	case fmt.Stringer:
		i, err = strconv.ParseInt(v.String(), 10, 64)
	}
	if err != nil {
		return 0, errors.New("integer value expected")
	}
	return
}
