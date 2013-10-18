package fb

type Index struct {
	Name       string
	TableName  string
	Unique     NullableBool
	Descending NullableBool
	Columns    []string
}
