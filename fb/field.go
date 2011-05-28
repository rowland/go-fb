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
