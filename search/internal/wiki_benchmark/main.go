package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	directory = "directory"
)

func main() {
	mode := flag.String("mode", "", "Mode to run: index or search")

	flag.Parse()

	switch *mode {
	case "index":
		_index()
	case "search":
		_search()
	default:
		fmt.Println("Usage: go run main.go -mode=index|search")
		os.Exit(1)
	}
}
