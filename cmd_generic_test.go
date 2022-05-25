package rediss

import (
	"fmt"
	"testing"
	"time"

	"github.com/pyihe/go-pkg/serialize"
	"github.com/pyihe/rediss/model/generic"
)

func printArray(prefix string, data *Reply) {
	if data == nil {
		fmt.Printf("%s%v", prefix, data)
		fmt.Println()
		return
	}
	if err := data.Error(); err != nil {
		fmt.Printf("%s%v", prefix, err)
		fmt.Println()
		return
	}
	if str := data.GetString(); str != "" {
		fmt.Printf("%s%v", prefix, str)
		fmt.Println()
		return
	}
	if len(data.GetArray()) > 0 {
		for _, arr := range data.GetArray() {
			printArray(prefix+" ", arr)
		}
	}
}

var (
	ops = []Option{
		WithDatabase(1),
		WithAddress("192.168.1.77:6379"),
		WithPassword("tB5PV~i$7U"),
		WithPoolSize(8),
		WithSerializer(serialize.JSON()),
		WithReadTimeout(1000 * time.Microsecond),
		WithWriteTimeout(1000 * time.Millisecond),
	}
	c = New(ops...)
)

func TestClient_Copy(t *testing.T) {
	testdata := []struct {
		src     string
		dst     string
		db      int
		replace bool
	}{
		{"btm", "dst1", 1, true},
		{"city", "dst2", -1, false},
	}
	for _, data := range testdata {
		reply, err := c.Copy(data.src, data.dst, data.db, data.replace)
		if err != nil {
			t.Fatalf("copy err: %v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_Migrate(t *testing.T) {
	testdata := []*generic.MigrateOption{
		{
			Host:        "192.168.1.77",
			Port:        "6379",
			Keys:        []string{"city"},
			Destination: 0,
			Timeout:     1,
			Copy:        true,
			Replace:     true,
		},
		{
			Host:        "192.168.1.77",
			Port:        "6379",
			Keys:        []string{"city", "btm"},
			Destination: 0,
			Timeout:     1,
			Copy:        true,
			Replace:     true,
		},
		{
			Host:        "192.168.1.77",
			Port:        "6379",
			Keys:        []string{"city", "btm", ""},
			Destination: 0,
			Timeout:     1,
			Copy:        true,
			Replace:     true,
		},
	}
	for _, v := range testdata {
		reply, err := c.Migrate(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_ObjectEncoding(t *testing.T) {
	testdata := []string{"city", "btm", "s1", "", " "}
	for _, v := range testdata {
		reply, err := c.ObjectEncoding(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_ObjectFreq(t *testing.T) {
	testdata := []string{"city", "btm", "s1", "", " "}
	for _, v := range testdata {
		reply, err := c.ObjectFreq(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_ObjectHelp(t *testing.T) {
	reply, err := c.ObjectHelp()
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	printArray(" ", reply)
}

func TestClient_ObjectIdleTime(t *testing.T) {
	testdata := []string{"city", "btm", "s1", "", " "}
	for _, v := range testdata {
		reply, err := c.ObjectIdleTime(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_ObjectRefCount(t *testing.T) {
	testdata := []string{"city", "btm", "s1", "", " "}
	for _, v := range testdata {
		reply, err := c.ObjectRefCount(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}

func TestClient_Restore(t *testing.T) {
	data := make(map[string]string)
	testdata := []string{"", "city", "btm"}
	for _, v := range testdata {
		reply, err := c.Dump(v)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
		data[v] = reply.GetString()
	}

	for k, v := range data {
		opt := &generic.RestoreOption{
			TTL:     time.Now().Add(5 * time.Minute).UnixMilli(),
			Replace: true,
			ABSTTL:  true,
		}
		reply, err := c.Restore(k+"_dump", v, opt)
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		printArray(" ", reply)
	}
}
