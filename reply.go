package rediss

import (
	"fmt"
	"strconv"

	"github.com/pyihe/go-pkg/bytes"
	"github.com/pyihe/go-pkg/errors"
	"github.com/pyihe/go-pkg/serialize"
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

func (r *Reply) isEmpty() bool {
	return len(r.array) == 0 && len(r.value) == 0 && r.err == nil
}

func (r *Reply) GetArray() []*Reply {
	return r.array
}

func (r *Reply) GetString() (s string) {
	s = bytes.String(r.value)
	return
}

func (r *Reply) GetBytes() []byte {
	b := make([]byte, len(r.value))
	copy(b, r.value)
	return b
}

func (r *Reply) GetInteger() (v int64, err error) {
	if data := r.value; len(data) > 0 {
		v, err = bytes.Int64(r.value)
	}
	return
}

func (r *Reply) GetFloat() (v float64, err error) {
	return strconv.ParseFloat(bytes.String(r.value), 64)
}

func (r *Reply) Error() (err error) {
	return r.err
}

func (r *Reply) Unmarshal(serializer serialize.Serializer, dst interface{}) error {
	return serializer.Decode(r.value, dst)
}

// Just for test
func (r *Reply) print(prefix string) {
	if r == nil {
		fmt.Printf("%s%v", prefix, r)
		fmt.Println()
		return
	}
	if err := r.Error(); err != nil {
		fmt.Printf("%s%v", prefix, err)
		fmt.Println()
		return
	}
	if str := r.GetString(); str != "" {
		fmt.Printf("%s%v", prefix, str)
		fmt.Println()
		return
	}
	for _, arr := range r.GetArray() {
		arr.print(fmt.Sprintf("%v ", prefix))
	}
}
