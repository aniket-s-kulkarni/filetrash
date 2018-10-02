package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
)

var Chunk = flag.Int64("chunk-size", 1024*1024*1024*5, "The file chunk size")
var Dir = flag.String("dir", "/tmp", "The directory to create files in")
var Threads = flag.Int("threads", 4, "Number of threads to create")
var Iterations = flag.Int("iter", 10, "Number of threads to iterations")

type RW struct {
	path string
}

func (r *RW) DoReads() {
	if r.path == "" {
		return
	}

	if f, err := os.Open(r.path); err == nil {
		defer f.Close()
		if info, err := f.Stat(); err == nil && info.Size() >= *Chunk {
			// read the file so unlink it
			os.Remove(r.path)
		}
	}
}

func (r *RW) DoWrites() {
	if r.path == "" {
		return
	}

	var f *os.File
	if _, err := os.Stat(r.path); err != nil {
		if f, err = os.Create(r.path); err != nil {
			fmt.Println("Couldn't create ", r.path, " ", err)
			r.path = ""
		} else {
			defer f.Close()
		}
	}

	if f != nil {
		f.WriteAt([]byte{0x0}, *Chunk)
		f.Sync()
	}
}

func (r *RW) Run() {
	for i := 0; i < *Iterations; i++ {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			r.DoReads()
			wg.Done()
		}()

		go func() {
			r.DoWrites()
			wg.Done()
		}()

		wg.Wait()
		fmt.Printf("[%d / %d] %s done\n", i, *Iterations, r.path)
	}
}

func newRW(path string, suffix int) *RW {
	path = fmt.Sprintf("%s.%d", path, suffix)
	return &RW{path}
}

func main() {
	flag.Parse()

	for i := 0; i < *Threads; i++ {
		rw := newRW(*Dir+"/test", i)
		fmt.Println("Starting read/writes on ", rw.path)
		go rw.Run()
	}
}
