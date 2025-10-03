package cli

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type cliCommand struct {
	name        string
	description string
	callback    func(*DatabaseConfig, []string) error
}

var commandRegistry map[string]cliCommand

func init() {
	commandRegistry = map[string]cliCommand{
		".help": {
			name:        ".help",
			description: "Displays a help message.",
			callback:    commandHelp,
		},
		".exit": {
			name:        ".exit",
			description: "Exit the program",
			callback:    commandExit,
		},
		"insert": {
			name:        "insert",
			description: "Perform a SQL INSERT action",
			callback:    commandInsert,
		},
		"select": {
			name:        "select",
			description: "Perform a SQL SELECT action",
			callback:    commandSelect,
		},
		"put": {
			name:        "put",
			description: "insert a value to the db",
			callback:    commandPut,
		},
		"get": {
			name:        "get",
			description: "get a value from the db",
			callback:    commandGet,
		},
	}
}

func commandHelp(config *DatabaseConfig, params []string) error {
	fmt.Println("Welcome to Go-DB!")
	fmt.Println("Usage: ")
	fmt.Println()

	for name, cmd := range commandRegistry {
		fmt.Printf("%s: %s\n", name, cmd.description)
	}
	return nil
}

func commandExit(config *DatabaseConfig, params []string) error {
	fmt.Println("Closing Go-DB... goodbye!")
	os.Exit(0)
	return nil
}

func commandPut(config *DatabaseConfig, params []string) error {
	// initial persistent storage technique

	if len(params) != 2 {
		return errors.New("you must provide a key and a value")
	}

	key, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}
	value, err := strconv.Atoi(params[1])
	if err != nil {
		return err
	}
	return config.KeyValue.Put(int32(key), int32(value))
}

func commandGet(config *DatabaseConfig, params []string) error {
	// initial persistent retrieval technique

	if len(params) != 1 {
		return errors.New("you must provide a key")
	}

	key, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}

	val, ok := config.KeyValue.Get(int32(key))
	if !ok {
		return errors.New("key not found")
	}
	fmt.Printf("%d -> %d\n", key, val)
	return nil
}

func commandInsert(config *DatabaseConfig, params []string) error {
	// insert into TableStore: insert id name age
	// example: insert 1 Alice 30

	if len(params) < 3 {
		return fmt.Errorf("need 3 parameters: id name age")
	}

	id, err := strconv.Atoi(params[0])
	if err != nil {
		return fmt.Errorf("invalid id: %v", err)
	}

	age, err := strconv.Atoi(params[2])
	if err != nil {
		return fmt.Errorf("invalid age: %v", err)
	}

	record := Record{
		"id":   int32(id),
		"name": params[1],
		"age":  int32(age),
	}

	return config.TableS.Insert(record)
}

func commandSelect(config *DatabaseConfig, params []string) error {
	records, err := config.TableS.ScanAll()
	if err != nil {
		return err
	}

	for i, record := range records {
		fmt.Printf("%d: id=%v name=%v age=%v\n", i, record["id"], record["name"], record["age"])
	}

	return nil
}
