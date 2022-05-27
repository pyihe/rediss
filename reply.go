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

func (reply *Reply) GetArray() []*Reply {
	return reply.array
}

func (reply *Reply) GetString() (s string) {
	s = bytes.String(reply.value)
	return
}

func (reply *Reply) GetBytes() []byte {
	return reply.value
}

func (reply *Reply) GetInteger() (v int64, err error) {
	v, err = bytes.Int64(reply.value)
	return
}

func (reply *Reply) GetFloat() (v float64, err error) {
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
	array := reply.GetArray()
	if len(array) != 2 {
		return
	}
	keysArray := array[1].GetArray()
	result = &generic.ScanResult{
		Keys: make([]string, 0, len(keysArray)),
	}
	result.Cursor, _ = array[0].GetInteger()
	for _, k := range keysArray {
		result.Keys = append(result.Keys, k.GetString())
	}
	return
}

// 解析GEO获取位置相关命令的结果
func (reply *Reply) parseGeoLocation(option interface{}) (result []*geo.Location, err error) {
	array := reply.GetArray()
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
	for _, arr := range reply.GetArray() {
		// 如果每个点都是多元素组成的数组
		subArr := arr.GetArray()
		location := &geo.Location{}

		switch len(subArr) {
		case 0:
			location.Name = arr.GetString()
		case 1:
		case 2:
			location.Name = subArr[0].GetString()
			if withDist {
				location.Distance, _ = subArr[1].GetFloat()
			}
			if withHash {
				location.GeoHash, _ = subArr[1].GetInteger()
			}
			if withCoord {
				location.Longitude, _ = subArr[1].GetArray()[0].GetFloat()
				location.Latitude, _ = subArr[1].GetArray()[1].GetFloat()
			}
		case 3:
			location.Name = subArr[0].GetString()
			if !withCoord {
				location.Distance, _ = subArr[1].GetFloat()
				location.GeoHash, _ = subArr[2].GetInteger()
			}
			if !withHash {
				location.Distance, _ = subArr[1].GetFloat()
				location.Longitude, _ = subArr[2].GetArray()[0].GetFloat()
				location.Latitude, _ = subArr[2].GetArray()[1].GetFloat()
			}
			if !withDist {
				location.GeoHash, _ = subArr[1].GetInteger()
				location.Longitude, _ = subArr[2].GetArray()[0].GetFloat()
				location.Latitude, _ = subArr[2].GetArray()[1].GetFloat()
			}
		case 4:
			location.Name = subArr[0].GetString()
			location.Distance, _ = subArr[1].GetFloat()
			location.GeoHash, _ = subArr[2].GetInteger()
			location.Longitude, _ = subArr[3].GetArray()[0].GetFloat()
			location.Latitude, _ = subArr[3].GetArray()[1].GetFloat()
		}
		result = append(result, location)
	}
	return
}

// 解析GEOPOS命令的结果
func (reply *Reply) parseGeoPosResult(members ...string) (result []*geo.Location, err error) {
	result = make([]*geo.Location, len(members))
	for i, arr := range reply.GetArray() {
		if arr != nil {
			var subArr = arr.GetArray()
			var location = &geo.Location{Name: members[i]}

			if location.Longitude, err = subArr[0].GetFloat(); err != nil {
				return
			}
			if location.Latitude, err = subArr[1].GetFloat(); err != nil {
				return
			}
			result[i] = location
		}
	}
	return
}

// 解析HSCAN命令的结果
func (reply *Reply) parseHScanResult() (result *hash.ScanResult, err error) {
	var array = reply.GetArray()
	if len(array) != 2 {
		return
	}
	var fvArray = array[1].GetArray()
	result = &hash.ScanResult{FieldValues: hash.NewFieldValue()}
	result.Cursor, _ = array[0].GetInteger()
	for i := 0; i < len(fvArray)-1; i += 2 {
		field := fvArray[i].GetString()
		value := fvArray[i+1].GetString()
		result.FieldValues.Set(field, value)
	}
	return
}

// 解析HRANDFIELD命令的结果
func (reply *Reply) parseHRandFieldResult(count int64, withValues bool) (result hash.FieldValue, err error) {
	result = hash.NewFieldValue()
	switch count {
	case 0:
		result.Set(reply.GetString(), nil)
	default:
		array := reply.GetArray()
		if withValues {
			for i := 0; i < len(array)-1; i += 2 {
				field := array[i].GetString()
				value := array[i+1].GetBytes()
				result.Set(field, value)
			}
		} else {
			for _, k := range array {
				result.Set(k.GetString(), nil)
			}
		}
	}
	return
}

//解析HGETALL命令的结果
func (reply *Reply) parseHGetAllResult() (result hash.FieldValue, err error) {
	result = hash.NewFieldValue()
	var fieldArray = reply.GetArray()
	for i := 0; i < len(fieldArray)-1; i += 2 {
		field := fieldArray[i].GetString()
		value := fieldArray[i+1].GetBytes()
		result.Set(field, value)
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
	if str := reply.GetString(); str != "" {
		fmt.Printf("%s%v", prefix, str)
		fmt.Println()
		return
	}
	for _, arr := range reply.GetArray() {
		arr.print(fmt.Sprintf("%v ", prefix))
	}
}
