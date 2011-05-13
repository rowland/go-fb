package fb

import "testing"
import "fmt"

type SuperTest struct {
	t      *testing.T
	prefix string
}

func (st *SuperTest) Equal(a interface{}, b interface{}) {
	if a != b {
		st.t.Error(fmt.Sprintf("%s: Expected %v, got %v", st.prefix, a, b))
	}
}
