package hash

import "github.com/pyihe/go-pkg/maps"

type FieldValue = maps.Param

func NewFieldValue() FieldValue {
	return maps.NewParam()
}

// ScanResult HSCAN命令的回复
type ScanResult struct {
	Cursor      int64
	FieldValues FieldValue
}
