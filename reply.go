package rediss

import (
	"fmt"
	"strconv"

	"github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/go-pkg/serialize"
	"github.com/pyihe/rediss/model/generic"
	"github.com/pyihe/rediss/model/geo"
	"github.com/pyihe/rediss/model/hash"
	"github.com/pyihe/rediss/model/list"
	"github.com/pyihe/rediss/model/set"
)

// Reply load parsed reply from redis server
type Reply struct {
	array []*Reply // nested array
	value []byte   // SimpleString & Integer & BulkString
	err   error    // Error
}

func newReply(b []byte, err ...string) (reply *Reply) {
	reply = &Reply{
		value: b,
	}
	if len(err) > 0 {
		reply.err = errors.New(err[0])
	}
	return
}

func (reply *Reply) Array() []*Reply {
	return reply.array
}

func (reply *Reply) ToString() (s string) {
	s = bytes.String(reply.value)
	return
}

func (reply *Reply) Bool() (b bool, err error) {
	return strconv.ParseBool(reply.ToString())
}

func (reply *Reply) Bytes() []byte {
	return reply.value
}

func (reply *Reply) Integer() (v int64, err error) {
	v, err = bytes.Int64(reply.value)
	return
}

func (reply *Reply) Float() (v float64, err error) {
	return strconv.ParseFloat(bytes.String(reply.value), 64)
}

func (reply *Reply) Error() (err error) {
	return reply.err
}

func (reply *Reply) Unmarshal(serializer serialize.Serializer, dst interface{}) error {
	return serializer.Decode(reply.value, dst)
}

/************************************************************************************************************/

// 解析命令SCAN的结果
func (reply *Reply) parseScanResult() (result *generic.ScanResult, err error) {
	// SCAN命令回复格式: 长度为2的数组
	// 数组第一个元素为返回的cursor
	// 第二个元素是由返回的key组成的数组
	array := reply.Array()
	if len(array) != 2 {
		return
	}
	keysArray := array[1].Array()
	result = &generic.ScanResult{
		Keys: make([]string, 0, len(keysArray)),
	}
	result.Cursor, _ = array[0].Integer()
	for _, k := range keysArray {
		result.Keys = append(result.Keys, k.ToString())
	}
	return
}

// 解析GEO获取位置相关命令的结果
func (reply *Reply) parseGeoLocation(option interface{}) (result []*geo.Location, err error) {
	array := reply.Array()
	n := len(array)
	if n == 0 {
		return
	}
	var withDist, withHash, withCoord bool
	switch opt := option.(type) {
	case *geo.RadiusOption:
		withDist, withHash, withCoord = opt.WithDist, opt.WithHash, opt.WithCoord
	case *geo.SearchOption:
		withDist, withHash, withCoord = opt.WithDist, opt.WithHash, opt.WithCoord
	}

	result = make([]*geo.Location, 0, n)
	for _, arr := range reply.Array() {
		// 如果每个点都是多元素组成的数组
		subArr := arr.Array()
		location := &geo.Location{}

		switch len(subArr) {
		case 0:
			location.Name = arr.ToString()
		case 1:
		case 2:
			location.Name = subArr[0].ToString()
			if withDist {
				location.Distance, _ = subArr[1].Float()
			}
			if withHash {
				location.GeoHash, _ = subArr[1].Integer()
			}
			if withCoord {
				location.Longitude, _ = subArr[1].Array()[0].Float()
				location.Latitude, _ = subArr[1].Array()[1].Float()
			}
		case 3:
			location.Name = subArr[0].ToString()
			if !withCoord {
				location.Distance, _ = subArr[1].Float()
				location.GeoHash, _ = subArr[2].Integer()
			}
			if !withHash {
				location.Distance, _ = subArr[1].Float()
				location.Longitude, _ = subArr[2].Array()[0].Float()
				location.Latitude, _ = subArr[2].Array()[1].Float()
			}
			if !withDist {
				location.GeoHash, _ = subArr[1].Integer()
				location.Longitude, _ = subArr[2].Array()[0].Float()
				location.Latitude, _ = subArr[2].Array()[1].Float()
			}
		case 4:
			location.Name = subArr[0].ToString()
			location.Distance, _ = subArr[1].Float()
			location.GeoHash, _ = subArr[2].Integer()
			location.Longitude, _ = subArr[3].Array()[0].Float()
			location.Latitude, _ = subArr[3].Array()[1].Float()
		}
		result = append(result, location)
	}
	return
}

// 解析GEOPOS命令的结果
func (reply *Reply) parseGeoPosResult(members ...string) (result []*geo.Location, err error) {
	result = make([]*geo.Location, len(members))
	for i, arr := range reply.Array() {
		if arr != nil {
			var subArr = arr.Array()
			var location = &geo.Location{Name: members[i]}

			if location.Longitude, err = subArr[0].Float(); err != nil {
				return
			}
			if location.Latitude, err = subArr[1].Float(); err != nil {
				return
			}
			result[i] = location
		}
	}
	return
}

// 解析HSCAN命令的结果
func (reply *Reply) parseHScanResult() (result *hash.ScanResult, err error) {
	var array = reply.Array()
	if len(array) != 2 {
		return
	}
	var fvArray = array[1].Array()
	result = &hash.ScanResult{FieldValues: hash.NewFieldValue()}
	result.Cursor, _ = array[0].Integer()
	for i := 0; i < len(fvArray)-1; i += 2 {
		field := fvArray[i].ToString()
		value := fvArray[i+1].ToString()
		result.FieldValues.Set(field, value)
	}
	return
}

// 解析HRANDFIELD命令的结果
func (reply *Reply) parseHRandFieldResult(count int64, withValues bool) (result hash.FieldValue, err error) {
	result = hash.NewFieldValue()
	switch count {
	case 0:
		result.Set(reply.ToString(), nil)
	default:
		array := reply.Array()
		if withValues {
			for i := 0; i < len(array)-1; i += 2 {
				field := array[i].ToString()
				value := array[i+1].Bytes()
				result.Set(field, value)
			}
		} else {
			for _, k := range array {
				result.Set(k.ToString(), nil)
			}
		}
	}
	return
}

//解析HGETALL命令的结果
func (reply *Reply) parseHGetAllResult() (result hash.FieldValue, err error) {
	result = hash.NewFieldValue()
	var fieldArray = reply.Array()
	for i := 0; i < len(fieldArray)-1; i += 2 {
		field := fieldArray[i].ToString()
		value := fieldArray[i+1].Bytes()
		result.Set(field, value)
	}
	return
}

func (reply *Reply) parseMPopResult() (result *list.MPopResult, err error) {
	array := reply.Array()
	result = &list.MPopResult{
		Key: array[0].ToString(),
	}
	elementsArray := array[1].Array()
	result.Elements = make([]string, 0, len(elementsArray))
	for _, v := range elementsArray {
		result.Elements = append(result.Elements, v.ToString())
	}
	return
}

func (reply *Reply) parseBPopResult() (result *list.BPopResult, err error) {
	array := reply.Array()
	result = &list.BPopResult{
		Key:     array[0].ToString(),
		Element: array[1].ToString(),
	}
	return
}

func (reply *Reply) parsePopResult() (result []string, err error) {
	array := reply.Array()
	switch len(array) {
	case 0:
		result = make([]string, 1)
		result[0] = reply.ToString()
	default:
		result = make([]string, 0, len(array))
		for _, v := range array {
			result = append(result, v.ToString())
		}
	}
	return
}

func (reply *Reply) parseLPosResult() (result []int64, err error) {
	array := reply.Array()
	switch len(array) {
	case 0:
		result = make([]int64, 0, 1)
		pos, err := reply.Integer()
		if err != nil {
			return result, err
		}
		result = append(result, pos)
	default:
		result = make([]int64, 0, len(array))
		for _, v := range array {
			pos, err := v.Integer()
			if err != nil {
				return result, err
			}
			result = append(result, pos)
		}
	}
	return
}

func (reply *Reply) parseIsMember() (result []bool, err error) {
	array := reply.Array()
	result = make([]bool, len(array))
	for i, v := range array {
		result[i] = v.ToString() == "1"
	}
	return
}

func (reply *Reply) parseSScanResult() (result *set.ScanResult, err error) {
	array := reply.Array()
	result = &set.ScanResult{}
	result.Cursor, err = array[0].Integer()
	result.Members = make([]string, 0, len(array[1].Array()))
	for _, v := range array[1].Array() {
		result.Members = append(result.Members, v.ToString())
	}
	return
}

// Just for test
func (reply *Reply) print(prefix string) {
	if reply == nil {
		fmt.Printf("%s%v", prefix, reply)
		fmt.Println()
		return
	}
	if err := reply.Error(); err != nil {
		fmt.Printf("%s%v", prefix, err)
		fmt.Println()
		return
	}
	array := reply.Array()

	if str := reply.ToString(); str != "" || len(array) == 0 {
		fmt.Printf("%s%v", prefix, str)
		fmt.Println()
		return
	}
	for _, arr := range array {
		arr.print(fmt.Sprintf("%v ", prefix))
	}
}
