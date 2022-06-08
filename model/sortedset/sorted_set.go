package sortedset

type RangeOption struct {
	Min       interface{}
	Max       interface{}
	By        string
	Rev       bool
	Offset    int64
	Count     int64
	WithScore bool
}

// AddOption ZADD 选项
type AddOption struct {
	NxOrXX string
	GtOrLt string
	Ch     bool
	Incr   bool
}

type Member struct {
	Value interface{}
	Score float64
}

type PopResult struct {
	Key     string
	Members []Member
}

type ScanResult struct {
	Cursor  int64
	Members []Member
}
