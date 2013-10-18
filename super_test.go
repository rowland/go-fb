package fb

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type SuperTest struct {
	*testing.T
}

func (st *SuperTest) Equal(expected interface{}, actual interface{}) {
	if expected != actual {
		st.fail(expected, actual, false)
	}
}

func (st *SuperTest) MustEqual(expected interface{}, actual interface{}) {
	if expected != actual {
		st.fail(expected, actual, true)
	}
}

func (st *SuperTest) False(actual bool) {
	if actual {
		st.fail(false, true, false)
	}
}

func (st *SuperTest) True(actual bool) {
	if !actual {
		st.fail(true, false, false)
	}
}

func (st *SuperTest) Nil(actual interface{}) {
	if actual != nil {
		st.fail(nil, actual, false)
	}
}

func (st *SuperTest) fail(expected, actual interface{}, must bool) {
	pc, file, line, ok := runtime.Caller(2)
	var name string
	if ok {
		name = runtime.FuncForPC(pc).Name()
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		file = filepath.Base(file)
	} else {
		name = "unknown func"
		file = "unknown file"
		line = 1
	}
	if must {
		st.Fatalf("\t%s:%d: %s: Expected %v, got %v\n", file, line, name, expected, actual)
	} else {
		st.Errorf("\t%s:%d: %s: Expected %v, got %v\n", file, line, name, expected, actual)
	}
}
