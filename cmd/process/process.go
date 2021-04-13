package main

import (
	"os/exec"
	"path/filepath"
	processor "transcoding-processor"
)

/* input(file path) -ffmpeg->  extract a thumbnail. output(file )
 */
// write a function that take	s an entry? from a queue and just prints something related to it
// ffmpeg -ss 3 -i input.mp4 -vf "select=gt(scene\,0.4)" -vsync vfr -vf fps=fps=1/600 oust%02d.jpg

func getFfmpegCmd(path string) *exec.Cmd {

	op := path[:len(path)-len(filepath.Ext(path))] + ".jpg"

	c := exec.Command("ffmpeg", "-ss", "3", "-i", path, op)
	return c
}

func Process(e processor.Entry) {
	// folder/file.mp4
	getFfmpegCmd(e.Path)
}
