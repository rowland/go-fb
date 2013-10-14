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
		st.fail(expected, actual)
	}
}

func (st *SuperTest) False(actual bool) {
	if actual {
		st.fail(false, true)
	}
}

func (st *SuperTest) True(actual bool) {
	if !actual {
		st.fail(true, false)
	}
}

func (st *SuperTest) fail(expected, actual interface{}) {
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
	st.Errorf("\t%s:%d: %s: Expected %v, got %v\n", file, line, name, expected, actual)	
}
