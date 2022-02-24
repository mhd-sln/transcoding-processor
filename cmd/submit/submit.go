package main

import (
	"fmt"
	"os"
	processor "transcoding"
)

func Main() error {
	mAddr := os.Getenv("minioAddr")
	kAddr := os.Getenv("kafkaAddr")
	mAKey := os.Getenv("minioAccessKey")
	mPw := os.Getenv("minioPW")
	s := processor.WriteHandler("submissions", mAddr, mAKey, mPw, kAddr)
	for _, f := range os.Args[1:] {
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
