package hash

import "github.com/pyihe/go-pkg/maps"

type FieldValue = maps.Param

func NewFieldValue() FieldValue {
	return maps.NewParam()
}
