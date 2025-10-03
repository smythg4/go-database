package cli

import (
	"errors"
	"fmt"
	"godb/internal/schema"
	"godb/internal/store"
	"os"
	"strconv"
	"strings"
)

type DatabaseConfig struct {
	KeyValue *store.KVStore
	TableS   *store.TableStore
}

type CliCommand struct {
	Name        string
	Description string
	Callback    func(*DatabaseConfig, []string) error
}

var CommandRegistry map[string]CliCommand

func init() {
	CommandRegistry = map[string]CliCommand{
		".help": {
			Name:        ".help",
			Description: "Displays a help message.",
			Callback:    commandHelp,
		},
		".exit": {
			Name:        ".exit",
			Description: "Exit the program",
			Callback:    commandExit,
		},
		"insert": {
			Name:        "insert",
			Description: "Perform a SQL INSERT action",
			Callback:    commandInsert,
		},
		"select": {
			Name:        "select",
			Description: "Perform a SQL SELECT action",
			Callback:    commandSelect,
		},
		"put": {
			Name:        "put",
			Description: "insert a value to the db",
			Callback:    commandPut,
		},
		"get": {
			Name:        "get",
			Description: "get a value from the db",
			Callback:    commandGet,
		},
		"show": {
			Name:        "show",
			Description: "Show all tables",
			Callback:    commandShow,
		},
		"use": {
			Name:        "use",
			Description: "Select active table",
			Callback:    commandUse,
		},
		"create": {
			Name:        "create",
			Description: "Create a new table -- CREATE tablename field1name:field1type field2name:field2type ...",
			Callback:    commandCreate,
		},
	}
}

func commandCreate(config *DatabaseConfig, params []string) error {
	if len(params) < 2 {
		return errors.New("must provide at least a table name with a single field")
	}

	tName := params[0]
	fName := tName + ".db"

	fmt.Printf("New Filename: %s\n", fName)

	fields := make([]schema.Field, 0, len(params)-1)
	for _, paramPair := range params[1:] {
		parts := strings.Split(paramPair, ":")
		if len(parts) != 2 {
			return errors.New("error parsing fieldnames and types")
		}
		fieldName := parts[0]
		fieldType, err := schema.ParseFieldType(parts[1])
		if err != nil {
			return err
		}

		fields = append(fields, schema.Field{
			Name: fieldName,
			Type: fieldType,
		})
	}

	sch := schema.Schema{
		TableName: tName,
		Fields:    fields,
	}

	newTableStore, err := store.CreateTableStore(fName, sch)
	if err != nil {
		return err
	}
	config.TableS = newTableStore

	return nil
}

func commandUse(config *DatabaseConfig, params []string) error {
	if len(params) != 1 {
		return errors.New("must provide table name to use -- try SHOW first")
	}

	tName := params[0]

	ts, err := store.NewTableStore(tName + ".db")
	if err != nil {
		return err
	}
	config.TableS = ts
	return nil
}

func commandShow(config *DatabaseConfig, params []string) error {
	files, err := os.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".db") {
			fmt.Println(strings.TrimSuffix(file.Name(), ".db"))
		}

	}
	return nil
}

func commandHelp(config *DatabaseConfig, params []string) error {
	fmt.Println("Welcome to Go-DB!")
	fmt.Println("Usage: ")
	fmt.Println()

	for name, cmd := range CommandRegistry {
		fmt.Printf("%s: %s\n", name, cmd.Description)
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
	fieldCount := len(config.TableS.Schema.Fields)

	if len(params) != fieldCount {
		return fmt.Errorf("need %d parameters for fields: %v", fieldCount, config.TableS.Schema.GetFieldNames())
	}

	record := make(schema.Record)
	for i, field := range config.TableS.Schema.Fields {
		// parse params[i] according to field.type
		value, err := schema.ParseValue(params[i], field.Type)
		if err != nil {
			return fmt.Errorf("invalid value for %s: %v", field.Name, err)
		}
		record[field.Name] = value
	}

	return config.TableS.Insert(record)
}

func selectAll(config *DatabaseConfig) error {
	records, err := config.TableS.ScanAll()
	if err != nil {
		return err
	}

	for i, record := range records {
		fmt.Printf("%d: ", i)
		for _, field := range config.TableS.Schema.Fields {
			fmt.Printf("%s=%v ", field.Name, record[field.Name])
		}
		fmt.Println()
	}

	return nil
}

func commandSelect(config *DatabaseConfig, params []string) error {
	if len(params) == 0 {
		return selectAll(config)
	}

	id, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}

	record, err := config.TableS.Find(id)

	for _, field := range config.TableS.Schema.Fields {
		fmt.Printf("%s=%v ", field.Name, record[field.Name])
	}
	fmt.Println()

	return err
}
