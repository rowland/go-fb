package fb

type Field struct {
	Name         string
	TypeCode     int
	SqlType      string
	SqlSubtype   int
	DisplaySize  int
	InternalSize int
	Precision    int
	Scale        int
	Nullable     bool
}

func fieldsMapFromSlice(fields []*Field) map[string]*Field {
	m := make(map[string]*Field, len(fields))
	for _, f := range fields {
		m[f.Name] = f
	}
	return m
}
