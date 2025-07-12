package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func IsConnected(conn driver.Conn) error {
	return conn.Ping(context.Background())
}

func Connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Username: "demo",
				Password: "demo",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},
			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func CloseConnection(conn driver.Conn) error {
	return conn.Close()
}

func ListDatabases(conn driver.Conn) ([]string, error) {
	rows, err := conn.Query(context.Background(), "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		databases = append(databases, db)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return databases, nil
}

func CreateDatabase(conn driver.Conn, dbName string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	if err := conn.Exec(context.Background(), query); err != nil {
		fmt.Printf("[ ERROR ] : Failed to create %s - %v\n", dbName, err)
		return err
	}
	return nil
}

func DeleteDatabase(conn driver.Conn, dbName string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if err := conn.Exec(context.Background(), query); err != nil {
		fmt.Printf("[ ERROR ] : Failed to delete %s - %v\n", dbName, err)
		return err
	}
	return nil
}

func CheckIfTableExists(conn driver.Conn, dbName, tableName string) (bool, error) {
	query := `
		SELECT count()
		FROM system.tables
		WHERE database = ? AND name = ?
	`
	var count uint64
	if err := conn.QueryRow(context.Background(), query, dbName, tableName).Scan(&count); err != nil {
		fmt.Printf("[ ERROR ] : Failed to check if table exists - %v\n", err)
		return false, err
	}
	return count > 0, nil
}

// AddFieldToTable adds a new column to an existing table in the specified database.
func AddFieldToTable(conn driver.Conn, dbName, tableName, fieldName, fieldType string, defaultValue interface{}) error {
	query := fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` ADD COLUMN IF NOT EXISTS %s %s DEFAULT ?",
		dbName, tableName, fieldName, fieldType,
	)
	if err := conn.Exec(context.Background(), query, defaultValue); err != nil {
		fmt.Printf("[ ERROR ] : Failed to add field %s to %s in %s - %v\n", fieldName, tableName, dbName, err)
		return err
	}
	return nil
}

// InsertData inserts multiple rows into a table in the specified database.
// values should be a slice of map[string]interface{}, where each map represents a row.
func InsertData(conn driver.Conn, dbName, tableName string, values []map[string]interface{}) error {
	if len(values) == 0 {
		return nil
	}
	columns := make([]string, 0, len(values[0]))
	for col := range values[0] {
		columns = append(columns, col)
	}
	// Prepare the placeholders and data
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		dbName, tableName, strings.Join(columns, ","), strings.Join(placeholders, ","),
	)
	batch, err := conn.PrepareBatch(context.Background(), query)
	if err != nil {
		fmt.Printf("[ ERROR ] : Failed to prepare batch insert into %s in %s - %v\n", tableName, dbName, err)
		return err
	}
	for _, row := range values {
		args := make([]interface{}, len(columns))
		for i, col := range columns {
			args[i] = row[col]
		}
		if err := batch.Append(args...); err != nil {
			fmt.Printf("[ ERROR ] : Failed to append row for insert into %s in %s - %v\n", tableName, dbName, err)
			return err
		}
	}
	if err := batch.Send(); err != nil {
		fmt.Printf("[ ERROR ] : Failed to send batch insert into %s in %s - %v\n", tableName, dbName, err)
		return err
	}
	return nil
}
