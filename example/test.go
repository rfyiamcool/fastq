package main

import (
	"fmt"

	"github.com/rfyiamcool/fastq"
)

func main() {
	fastq.Init()
	msg := &fastq.Message{
		Module:     "test",
		Msg:        "i love u",
		RetryTimes: 1,
	}

	var count = 100
	for index := 0; index < count; index++ {
		err := fastq.Write(msg)
		fmt.Println(err)
	}

	for index := 0; index < count; index++ {
		module, message, err := fastq.Read()
		fmt.Println(index, module, message)
		if module != msg.Module {
			panic("err")
		}

		if message != msg.Msg {
			panic("err")
		}

		if err != nil {
			panic(err.Error())
		}
	}
}
