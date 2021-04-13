package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*func Test_getFfmpegCmd(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want *exec.Cmd
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFfmpegCmd(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFfmpegCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}*/

func Test_getFfmpegCmd_WithPath(t *testing.T) {
	ip := "folder/file.ext"
	c := getFfmpegCmd(ip)
	assert.Equal(t, "folder/file.jpg", c.Args[5])
}
