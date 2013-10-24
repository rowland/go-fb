package fb

import (
	"testing"
	"time"
)

func TestRow_Scan(t *testing.T) {
	st := SuperTest{t}

	r := Row{
		1,
		1,
		[]byte("BINARY BLOB CONTENTS"),
		123,
		1234,
		1234567890,
		123.24,
		123456789.24,
		"A",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		"A",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		"TEXT BLOB CONTENTS",
		"2013-10-10",
		"08:42:00",
		"2013-10-10 08:42:00",
		5.55,
		30303.33,
	}

	var (
		id     int64
		flag   bool
		binary []byte
		i      int
		i32    int32
		i64    int64
		f32    float32
		f64    float64
		c      string
		cs     string
		v      string
		vs     string
		m      string
		dt     time.Time
		tm     time.Time
		ts     time.Time
		n92    float64
		d92    float64
	)

	if err := r.Scan(&id, &flag, &binary, &i, &i32, &i64, &f32, &f64, &c, &cs, &v, &vs, &m, &dt, &tm, &ts, &n92, &d92); err != nil {
		t.Fatal(err)
	}

	dtExpected := time.Date(2013, 10, 10, 0, 0, 0, 0, time.Local)
	tmExpected := time.Date(1970, 1, 1, 8, 42, 0, 0, time.Local)
	tsExpected := time.Date(2013, 10, 10, 8, 42, 0, 0, time.Local)

	st.Equal(int64(1), id)
	st.Equal(true, flag)
	st.Equal("BINARY BLOB CONTENTS", string(binary))
	st.Equal(123, i)
	st.Equal(int32(1234), i32)
	st.Equal(int64(1234567890), i64)
	st.Equal(float32(123.24), f32)
	st.Equal(123456789.24, f64)
	st.Equal("A", c)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", cs)
	st.Equal("A", v)
	st.Equal("ABCDEFGHIJKLMNOPQRSTUVWXYZ", vs)
	st.Equal("TEXT BLOB CONTENTS", m)
	st.Equal(dtExpected, dt)
	st.Equal(tmExpected, tm)
	st.Equal(tsExpected, ts)
	st.Equal(5.55, n92)
	st.Equal(30303.33, d92)
}
