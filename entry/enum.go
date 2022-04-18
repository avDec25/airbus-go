package entry

type Enum interface {
	Name() string
	Ordinal() int
	Values() *[]string
}
