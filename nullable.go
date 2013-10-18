package fb

import (
	"time"
)

type NullableBool struct {
	Value bool
	Null  bool
}

func (n *NullableBool) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = false, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableBytes struct {
	Value []byte
	Null  bool
}

func (n *NullableBytes) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = nil, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableFloat32 struct {
	Value float32
	Null  bool
}

func (n *NullableFloat32) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = 0, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableFloat64 struct {
	Value float64
	Null  bool
}

func (n *NullableFloat64) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = 0, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableInt16 struct {
	Value int16
	Null  bool
}

func (n *NullableInt16) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = 0, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableInt32 struct {
	Value int32
	Null  bool
}

func (n *NullableInt32) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = 0, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableInt64 struct {
	Value int64
	Null  bool
}

func (n *NullableInt64) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = 0, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableString struct {
	Value string
	Null  bool
}

func (n *NullableString) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = "", true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}

type NullableTime struct {
	Value time.Time
	Null  bool
}

func (n *NullableTime) Scan(value interface{}) error {
	if value == nil {
		n.Value, n.Null = time.Time{}, true
		return nil
	}
	n.Null = false
	return ConvertValue(&n.Value, value)
}
