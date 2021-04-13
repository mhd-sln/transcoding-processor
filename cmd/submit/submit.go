package main

import (
	"fmt"
	"os"
	processor "transcoding-processor"
)

// processor, file upload to submitter with Minio ??
func Main() error {
	s := processor.NewSubmitter("submissions")
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
