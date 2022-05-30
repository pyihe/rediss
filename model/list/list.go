package list

type PosOption struct {
	Rank   int64
	Count  int64
	MaxLen int64
}

/******************************************************************************************/

// MPopResult 接收BLMPOP命令的返回值
type MPopResult struct {
	Key      string
	Elements []string
}

// BPopResult 接收BLPop命令的返回值
type BPopResult struct {
	Key     string
	Element string
}
