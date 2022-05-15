package main

import (
	"fmt"

	"github.com/pyihe/go-pkg/serialize"
	"github.com/pyihe/rediss"
)

var defaultPrefix = " "

func main() {
	opts := []rediss.Option{
		rediss.WithDatabase(1),
		rediss.WithAddress("192.168.1.192:6379"),
		rediss.WithPassword("tB5PV~i$7U"),
		rediss.WithPoolSize(8),
		rediss.WithSerializer(serialize.JSON()),
	}
	c := rediss.New(opts...)

	reply, err := c.DelKey("*")
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	printArray(defaultPrefix, reply)
}

func printArray(prefix string, data *rediss.Reply) {
	if data == nil {
		fmt.Printf("%s%v", prefix, data)
		goto end
	}
	if str := data.GetString(); str != "" {
		fmt.Printf("%s%v", prefix, str)
		goto end
	}
	if err := data.Error(); err != nil {
		fmt.Printf("%s%v", prefix, err)
		goto end
	}
	for _, arr := range data.GetArray() {
		printArray(prefix+defaultPrefix, arr)
	}
end:
	fmt.Println()
}
