package main

import (
	"bufio"
	"fmt"
	"godb/internal/cli"
	"godb/internal/store"
	"log"
	"os"
	"strings"
)

func cleanInput(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	kv, err := store.NewKVStore("test.db")
	if err != nil {
		log.Fatal(err)
	}

	ts, err := store.NewTableStore("table.db")
	if err != nil {
		log.Fatal(err)
	}

	config := &cli.DatabaseConfig{
		KeyValue: kv,
		TableS:   ts,
	}

	for {
		fmt.Printf("Go-DB [%s]> ", config.TableS.File.Name())
		scanner.Scan()
		line := scanner.Text()
		clean_line := cleanInput(line)
		if len(clean_line) > 0 {
			// meta-command, use normal registry
			command, ok := cli.CommandRegistry[clean_line[0]]
			if ok {
				err := command.Callback(config, clean_line[1:])
				if err != nil {
					fmt.Printf("Error with command %s: %s\n", command.Name, err)
				}
			} else {
				fmt.Println("Unknown command")
			}

		} else {
			continue
		}
	}
}
