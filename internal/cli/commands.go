package cli

import (
	"errors"
	"fmt"
	"godb/internal/schema"
	"godb/internal/store"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	tableCacheMu sync.RWMutex
	tableCache   = make(map[string]*store.TableStore)
)

func GetOrOpenTable(filename string) (*store.TableStore, error) {
	tableCacheMu.Lock()
	defer tableCacheMu.Unlock()

	if ts, ok := tableCache[filename]; ok {
		return ts, nil
	}

	ts, err := store.NewTableStore(filename)
	if err != nil {
		return nil, err
	}
	tableCache[filename] = ts
	return ts, nil
}

func CreateTable(filename string, sch schema.Schema) (*store.TableStore, error) {
	tableCacheMu.Lock()
	defer tableCacheMu.Unlock()

	if _, ok := tableCache[filename]; ok {
		return nil, fmt.Errorf("table already open: %s", filename)
	}

	ts, err := store.CreateTableStore(filename, sch)
	if err != nil {
		return nil, err
	}

	tableCache[filename] = ts
	return ts, nil
}

type DatabaseConfig struct {
	KeyValue *store.KVStore
	TableS   *store.TableStore
}

type CliCommand struct {
	Name        string
	Description string
	Callback    func(*DatabaseConfig, []string, io.Writer) error
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

func commandCreate(config *DatabaseConfig, params []string, w io.Writer) error {
	if len(params) < 2 {
		return errors.New("must provide at least a table name with a single field")
	}

	tName := params[0]
	fName := tName + ".db"

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

	newTableStore, err := CreateTable(fName, sch)
	if err != nil {
		return err
	}
	config.TableS = newTableStore
	fmt.Fprintf(w, "New table created: %s\n", newTableStore.Schema.TableName)
	return nil
}

func commandUse(config *DatabaseConfig, params []string, w io.Writer) error {
	if len(params) != 1 {
		return errors.New("must provide table name to use -- try SHOW first")
	}

	tName := params[0]

	ts, err := GetOrOpenTable(tName + ".db")
	if err != nil {
		return err
	}
	config.TableS = ts
	fmt.Fprintf(w, "Switching to table: %s\n", tName)
	return nil
}

func commandShow(config *DatabaseConfig, params []string, w io.Writer) error {
	files, err := os.ReadDir(".")
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".db") {
			fmt.Fprintln(w, strings.TrimSuffix(file.Name(), ".db"))
		}

	}
	return nil
}

func commandHelp(config *DatabaseConfig, params []string, w io.Writer) error {
	fmt.Fprintln(w, "Welcome to Go-DB!")
	fmt.Fprintln(w, "Usage: ")
	fmt.Fprintln(w)

	for name, cmd := range CommandRegistry {
		fmt.Fprintf(w, "%s: %s\n", name, cmd.Description)
	}
	return nil
}

func commandExit(config *DatabaseConfig, params []string, w io.Writer) error {
	fmt.Fprintln(w, "Closing Go-DB... goodbye!")

	if conn, ok := w.(net.Conn); ok {
		return conn.Close()

	}
	defer os.Exit(0)
	for _, v := range tableCache {
		err := v.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func commandPut(config *DatabaseConfig, params []string, w io.Writer) error {
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
	fmt.Fprintf(w, "Adding %d -> %d to KV file", key, value)
	return config.KeyValue.Put(int32(key), int32(value))
}

func commandGet(config *DatabaseConfig, params []string, w io.Writer) error {
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
	fmt.Fprintf(w, "%d -> %d\n", key, val)
	return nil
}

func commandInsert(config *DatabaseConfig, params []string, w io.Writer) error {
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
	fmt.Fprintf(w, "Inserting %+v into table %s\n", record, config.TableS.Schema.TableName)
	return config.TableS.Insert(record)
}

func selectAll(config *DatabaseConfig, w io.Writer) error {
	records, err := config.TableS.ScanAll()
	if err != nil {
		return err
	}

	fieldNames := config.TableS.Schema.GetFieldNames()
	widths := make([]int, len(fieldNames))
	for i, field := range fieldNames {
		widths[i] = len(field) * 4
	}
	fmt.Fprint(w, "| ")
	for i, field := range fieldNames {
		fmt.Fprintf(w, "%-*s | ", widths[i], field)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("-", len(fieldNames)*20))

	for _, record := range records {
		fmt.Fprint(w, "| ")
		for i, field := range config.TableS.Schema.Fields {
			val := fmt.Sprintf("%v", record[field.Name])
			fmt.Fprintf(w, "%-*s | ", widths[i], val)
		}
		fmt.Fprintln(w)
	}

	return nil
}

func commandSelect(config *DatabaseConfig, params []string, w io.Writer) error {
	if len(params) == 0 {
		return selectAll(config, w)
	}

	fieldNames := config.TableS.Schema.GetFieldNames()
	widths := make([]int, len(fieldNames))
	for i, field := range fieldNames {
		widths[i] = len(field) * 4
	}
	fmt.Fprint(w, "| ")
	for i, field := range fieldNames {
		fmt.Fprintf(w, "%-*s | ", widths[i], field)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, strings.Repeat("-", len(fieldNames)*20))

	id, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}

	record, err := config.TableS.Find(id)

	fmt.Fprint(w, "| ")
	for i, field := range config.TableS.Schema.Fields {
		val := fmt.Sprintf("%v", record[field.Name])
		fmt.Fprintf(w, "%-*s | ", widths[i], val)
	}
	fmt.Fprintln(w)

	return err
}
