package main

import (
	"flag"
	"fmt"
	"os"
	processor "transcoding"
)

func Main() error {
	counts := flag.Int("count", 0, "count of submits")

	f := flag.Arg(0)
	flag.Parse()
	mAddr := os.Getenv("minioAddr")
	kAddr := os.Getenv("kafkaAddr")
	mAKey := os.Getenv("minioAccessKey")
	mPw := os.Getenv("minioPW")
	s := processor.WriteHandler("submissions", mAddr, mAKey, mPw, kAddr)

	for i := 0; i < *counts; i++ {
		m, err := s.Send(f)
		if err != nil {
			return err
		}
		fmt.Println(m.Id)
	}

	return nil
}

func main() {
	err := Main()
	if err != nil {
		fmt.Println(err.Error())
	}
}
