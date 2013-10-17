package fb

type Column struct {
	Name         string
	Domain       string
	SqlType      string
	SqlSubtype   NullableInt16
	Length       int16 // DisplaySize
	Precision    NullableInt16
	Scale        int16
	Default      NullableString
	Nullable     NullableBool
	TypeCode     int
	InternalSize int
}

func columnsMapFromSlice(cols []*Column) map[string]*Column {
	m := make(map[string]*Column, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}
