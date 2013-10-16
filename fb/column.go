package fb

type Column struct {
	Name         string
	Domain       string
	SqlType      string
	SqlSubtype   int
	Length       int // DisplaySize
	Precision    int
	Scale        int
	Default      *string
	Nullable     bool
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
