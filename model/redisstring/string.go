package redisstring

type SetOption struct {
	NxOrXX   string
	ExpireOp string // 有效期选项: EX/PX/EXAT/PXAT
	Expire   int64
	Get      bool
	KeepTTL  bool
}

type LCSResult struct {
	Len     int64
	Matches []LCSMatch
}

type LCSMatch struct {
	Len     int64
	Indexes []Index
}

type Index struct {
	Start int64
	Stop  int64
}
