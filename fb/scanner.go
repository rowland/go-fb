package fb

type Scanner interface {
	Scan(src interface{}) error
}
