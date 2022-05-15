package rediss

import (
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
	return r.value
}

func (r *Reply) GetInteger() (v int64, err error) {
	if data := r.value; len(data) > 0 {
		v, err = bytes.Int64(r.value)
	}
	return
}

func (r *Reply) Error() (err error) {
	return r.err
}

func (r *Reply) Unmarshal(serializer serialize.Serializer, dst interface{}) error {
	return serializer.Decode(r.value, dst)
}
