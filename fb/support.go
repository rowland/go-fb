package fb

import (
	"os"
	"strconv"
	"fmt"
)

func float64FromIf(v interface{}) (f float64, err os.Error) {
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
		f, err = strconv.Atof64(d)
	}
	if err != nil {
		return 0.0, os.NewError("numeric value expected")
	}
	return
}

func int64FromIf(v interface{}) (i int64, err os.Error) {
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
		i, err = strconv.Atoi64(v)
	case fmt.Stringer:
		i, err = strconv.Atoi64(v.String())
	}
	if err != nil {
		return 0, os.NewError("integer value expected")
	}
	return
}
