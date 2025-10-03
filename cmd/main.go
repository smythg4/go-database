package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

type DatabaseConfig struct {
	Table    *Table
	KeyValue *KVStore
	TableS   *TableStore
	// Future: connection settings, current database, transaction state, etc.
	// disk manager, BTree can go here too
}

func cleanInput(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	kv, err := NewKVStore("test.db")
	if err != nil {
		log.Fatal(err)
	}

	ts, err := NewTableStore("table.db")
	if err != nil {
		log.Fatal(err)
	}

	config := &DatabaseConfig{
		Table:    &Table{Rows: make([]Row, 0)},
		KeyValue: kv,
		TableS:   ts,
	}

	for {
		fmt.Print("Go-DB > ")
		scanner.Scan()
		line := scanner.Text()
		clean_line := cleanInput(line)
		if len(clean_line) > 0 {
			// meta-command, use normal registry
			command, ok := commandRegistry[clean_line[0]]
			if ok {
				err := command.callback(config, clean_line[1:])
				if err != nil {
					fmt.Printf("Error with command %s: %s\n", command.name, err)
				}
			} else {
				fmt.Println("Unknown command")
			}

		} else {
			continue
		}
	}
}
