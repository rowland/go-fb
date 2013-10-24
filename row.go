package fb

import (
	"fmt"
	"reflect"
)

type Row []interface{}

func (row Row) Scan(dest ...interface{}) error {
	if len(dest) != len(row) {
		return fmt.Errorf("fb: expected %d destination arguments to Scan, received %d", len(row), len(dest))
	}
	for i, v := range row {
		if err := ConvertValue(dest[i], v); err != nil {
			return fmt.Errorf("fb: Scan error on column %d: %v (%v, %v)", i, err, v, reflect.TypeOf(v))
		}
	}
	return nil
}
