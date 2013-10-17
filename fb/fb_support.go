package fb

/*
#include <ibase.h>
*/
import "C"

import (
	"bytes"
	"fmt"
	"time"
)

const (
	secsPerDay                           = 24 * 60 * 60
	daysFromModifiedJulianDayToUnixEpoch = 40587 // 17 Nov 1858 to 1 Jan 1970
	secsFromModifiedJulianDayToUnixEpoch = daysFromModifiedJulianDayToUnixEpoch * secsPerDay
)

func fbAlign(n C.ISC_SHORT, b C.ISC_SHORT) C.ISC_SHORT {
	return (n + b - 1) & ^(b - 1)
}

func fbErrorMsg(isc_status *C.ISC_STATUS) string {
	var msg [1024]C.ISC_SCHAR
	var buf bytes.Buffer
	for C.fb_interpret(&msg[0], 1024, &isc_status) != 0 {
		for i := 0; msg[i] != 0; i++ {
			buf.WriteByte(uint8(msg[i]))
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func fbErrorCheck(isc_status *[20]C.ISC_STATUS) error {
	if isc_status[0] == 1 && isc_status[1] != 0 {
		var msg [1024]C.ISC_SCHAR
		var code C.short = C.short(C.isc_sqlcode(&isc_status[0]))

		C.isc_sql_interprete(code, &msg[0], 1024)
		var buf bytes.Buffer
		for i := 0; msg[i] != 0; i++ {
			buf.WriteByte(uint8(msg[i]))
		}
		buf.WriteString("\n")
		buf.WriteString(fbErrorMsg(&isc_status[0]))

		return &Error{int(code), buf.String()}
	}
	return nil
}

func fbErrorCheckWarn(isc_status *[20]C.ISC_STATUS) error {
	var code C.short = C.short(C.isc_sqlcode(&isc_status[0]))
	if code != 0 {
		var buf [1024]C.ISC_SCHAR
		C.isc_sql_interprete(code, &buf[0], 1024)
		var msg bytes.Buffer
		for i := 0; buf[i] != 0; i++ {
			msg.WriteByte(uint8(buf[i]))
		}
		return &Error{int(code), msg.String()}
	}
	return nil
}

func timeFromIscTime(tm C.ISC_TIME, loc *time.Location) (t time.Time) {
	unixTimeSecs := int64(tm) / 10000
	unixFracSecs := int64(tm) % 10000
	ns := unixFracSecs * 100000
	unixTime := unixTimeSecs
	t = time.Unix(unixTime, ns).In(time.UTC)
	if loc != time.UTC {
		y, m, d := t.Date()
		h, n, s := t.Clock()
		t = time.Date(y, m, d, h, n, s, t.Nanosecond(), loc)
	}
	return
}

func timeFromIscDate(dt C.ISC_DATE, loc *time.Location) (t time.Time) {
	unixDaySecs := (int64(dt) * secsPerDay) - secsFromModifiedJulianDayToUnixEpoch
	unixTime := unixDaySecs
	t = time.Unix(unixTime, 0).In(time.UTC)
	if loc != time.UTC {
		y, m, d := t.Date()
		h, n, s := t.Clock()
		t = time.Date(y, m, d, h, n, s, t.Nanosecond(), loc)
	}
	return
}

func timeFromTimestamp(ts C.ISC_TIMESTAMP, loc *time.Location) (t time.Time) {
	unixDaySecs := (int64(ts.timestamp_date) * secsPerDay) - secsFromModifiedJulianDayToUnixEpoch
	unixTimeSecs := int64(ts.timestamp_time) / 10000
	unixFracSecs := int64(ts.timestamp_time) % 10000
	ns := unixFracSecs * 100000
	unixTime := unixDaySecs + unixTimeSecs
	t = time.Unix(unixTime, ns).In(time.UTC)
	if loc != time.UTC {
		y, m, d := t.Date()
		h, n, s := t.Clock()
		t = time.Date(y, m, d, h, n, s, t.Nanosecond(), loc)
	}
	return
}

func timestampFromTime(t time.Time, loc *time.Location) (ts C.ISC_TIMESTAMP) {
	if loc != time.UTC {
		y, m, d := t.Date()
		h, n, s := t.Clock()
		t = time.Date(y, m, d, h, n, s, t.Nanosecond(), time.UTC)
	}
	unix_days := t.Unix() / secsPerDay
	unix_secs := t.Unix() % secsPerDay
	ts.timestamp_date = C.ISC_DATE(unix_days + daysFromModifiedJulianDayToUnixEpoch)
	ts.timestamp_time = C.ISC_TIME(unix_secs*10000 + int64(t.Nanosecond())/100000)
	return
}

func iscTimeFromTime(t time.Time, loc *time.Location) (tm C.ISC_TIME) {
	if loc != time.UTC {
		y, m, d := t.Date()
		h, n, s := t.Clock()
		t = time.Date(y, m, d, h, n, s, t.Nanosecond(), time.UTC)
	}
	unix_secs := t.Unix() % secsPerDay
	tm = C.ISC_TIME(unix_secs*10000 + int64(t.Nanosecond())/100000)
	return
}

func sqlTypeFromCode(code, subType int) string {
	switch code {
	case C.SQL_TEXT, C.blr_text:
		return "CHAR"
	case C.SQL_VARYING, C.blr_varying:
		return "VARCHAR"
	case C.SQL_SHORT, C.blr_short:
		switch subType {
		case 0:
			return "SMALLINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case C.SQL_LONG, C.blr_long:
		switch subType {
		case 0:
			return "INTEGER"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
		break
	case C.SQL_FLOAT, C.blr_float:
		return "FLOAT"
	case C.SQL_DOUBLE, C.blr_double:
		switch subType {
		case 0:
			return "DOUBLE PRECISION"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	case C.SQL_D_FLOAT, C.blr_d_float:
		return "DOUBLE PRECISION"
	case C.SQL_TIMESTAMP, C.blr_timestamp:
		return "TIMESTAMP"
	case C.SQL_BLOB, C.blr_blob:
		return "BLOB"
	case C.SQL_ARRAY:
		return "ARRAY"
	case C.SQL_QUAD, C.blr_quad:
		return "DECIMAL"
	case C.SQL_TYPE_TIME, C.blr_sql_time:
		return "TIME"
	case C.SQL_TYPE_DATE, C.blr_sql_date:
		return "DATE"
	case C.SQL_INT64, C.blr_int64:
		switch subType {
		case 0:
			return "BIGINT"
		case 1:
			return "NUMERIC"
		case 2:
			return "DECIMAL"
		}
	}
	return fmt.Sprintf("UNKNOWN %d, %d", code, subType)
}

func precisionFromSqlvar(sqlvar *C.XSQLVAR) int16 {
	switch sqlvar.sqltype & ^1 {
	case C.SQL_SHORT:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 4
		case 2:
			return 4
		}
	case C.SQL_LONG:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 9
		case 2:
			return 9
		}
	case C.SQL_DOUBLE, C.SQL_D_FLOAT:
		switch sqlvar.sqlsubtype {
		case 0:
			return -1
		case 1:
			return 15
		case 2:
			return 15
		}
	case C.SQL_INT64:
		switch sqlvar.sqlsubtype {
		case 0:
			return 0
		case 1:
			return 18
		case 2:
			return 18
		}
		break
	}
	return -1
}
