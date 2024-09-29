package main

import (
	"log"
	"os"
	"runtime/pprof"
)

func startCpuProfiler(filename string) func() {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}

	return func() {
		pprof.StopCPUProfile()
		err := f.Close()
		if err != nil {
			log.Fatal("could not close profile file: ", err)
		}
	}
}
