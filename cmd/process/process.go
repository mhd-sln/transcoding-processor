package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	processor "transcoding"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type job struct {
	entry processor.Entry
}

type handler interface {
	Process(processor.Entry, int) (string, error)
	UploadResolve(string) error
}

type server struct {
	worker_count int
	p_handler    handler
	j            chan job
	workers      []*workerHandle
}

type workerHandle struct {
	id   int
	done chan struct{}
}

func main() {
	mAddr := os.Getenv("minioAddr")
	kAddr := os.Getenv("kafkaAddr")
	mAKey := os.Getenv("minioAccessKey")
	mPw := os.Getenv("minioPW")

	fmt.Println(mAddr)
	fmt.Println(kAddr)
	s := processor.ReadHandler("submissions", "video-processor", mAddr, mAKey, mPw, kAddr)

	serv := &server{
		p_handler:    s,
		worker_count: 1,
		j:            make(chan job),
	}

	go func() {
		c := make(chan os.Signal, 4)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		s.Close()
		fmt.Println("Closed the process")
		os.Exit(1)
	}()

	webserver(serv)

	for w := 1; w <= serv.worker_count; w++ {
		wh := &workerHandle{
			id:   w,
			done: make(chan struct{}),
		}
		go worker(serv, wh)
		serv.workers = append(serv.workers, wh)

	}
	for {
		e, err := s.Receive()
		if err != nil {
			fmt.Printf("Error in receiving an entry to a topic: %v\n", err)
			continue
		}
		serv.j <- job{*e}
	}

}

func worker(w *server, wh *workerHandle) {
	for {
		select {
		case <-wh.done:
			fmt.Println("Done received from worker num ", wh.id)
			return
		case job := <-w.j:
			start := time.Now()

			e := job.entry

			fn, err := w.p_handler.Process(e, wh.id)
			if err != nil {
				fmt.Printf("Error in processing the entry: %v\n", err)
				continue
			}
			err = w.p_handler.UploadResolve(fn)
			if err != nil {
				fmt.Printf("Error in uploading the output to minio: %v\n", err)
				continue
			}
			os.Remove(fn)
			fmt.Println("Upload success. Worker num", fn, wh.id, job.entry.Id)
			processor.AppDuration.Observe(float64(time.Since(start)) / float64(time.Second))
		}
	}
}

func (serv *server) workerHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fmt.Fprintln(w, "The number of workers are ", serv.worker_count)
		return
	case http.MethodPost:

		b := r.Body
		defer b.Close()

		bs, err := io.ReadAll(b)
		if err != nil {
			http.Error(w, "Error reading the POST body", 500)
			return
		}
		w_count, err := strconv.Atoi(string(bs))

		if err != nil {
			http.Error(w, "Error parsing", 500)
			return
		}

		if w_count == 0 {
			http.Error(w, "zero worker count", 500)
		}

		if w_count > serv.worker_count {
			for w := serv.worker_count; w <= w_count; w++ {
				wh := &workerHandle{
					id:   w,
					done: make(chan struct{}),
				}
				go worker(serv, wh)

				serv.workers = append(serv.workers, wh)
				fmt.Printf("Worker slice %#v \n", serv.workers)
			}
		} else {

			for _, w := range serv.workers {
				if w_count < w.id {
					fmt.Println("Send done to w.id : ", w.id)
					close(w.done)
				}
			}

			serv.workers = serv.workers[0 : w_count-1]
		}

		serv.worker_count = w_count

	}
}

func webserver(serv *server) {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/workers", serv.workerHandler)

	go http.ListenAndServe(":8081", nil)
}
