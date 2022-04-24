package main

import (
	"fmt"
	"time"

	"pkg/path"
)

func main() {
	begin := time.Now()

	// main
	err := path.ReadAndCreat()
	if err != nil {
		fmt.Println("readAndCreatOld err", err)
	}

	path.Result()

	cost := time.Since(begin)
	fmt.Println("\nprocess cost time:",cost)
}
