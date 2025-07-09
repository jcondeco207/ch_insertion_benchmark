package main

import (
	"fmt"

	"jcondeco.com/bm1/internal/clickhouse"
)

func main() {
	fmt.Println("Hello there")
	_, err := clickhouse.Connect()
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}

}
