package main

import (
	"fmt"

	"jcondeco.com/bm1/internal/clickhouse"
)

func main() {
	fmt.Println("Hello there")
	connection, err := clickhouse.Connect()
	if err != nil {
		fmt.Println("Failed to connect to clickhouse:", err)
		return
	}

	dbs, err := clickhouse.ListDatabases(connection)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(dbs)

}
