package sortedset

// AddOption ZADD 选项
type AddOption struct {
	NxOrXX string
	GtOrLt string
	Ch     bool
	Incr   bool
}

type Member struct {
	Score float64
	Value interface{}
}

type PopResult struct {
	Key     string
	Members []*Member
}
