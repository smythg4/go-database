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
	tableCache   = make(map[string]*store.BTreeStore)
)

func GetOrOpenTable(filename string) (*store.BTreeStore, error) {
	tableCacheMu.Lock()
	defer tableCacheMu.Unlock()

	if bts, ok := tableCache[filename]; ok {
		return bts, nil
	}

	bts, err := store.NewBTreeStore(filename)
	if err != nil {
		return nil, err
	}
	tableCache[filename] = bts
	return bts, nil
}

func CreateTable(filename string, sch schema.Schema) (*store.BTreeStore, error) {
	tableCacheMu.Lock()
	defer tableCacheMu.Unlock()

	if _, ok := tableCache[filename]; ok {
		return nil, fmt.Errorf("table already open: %s", filename)
	}

	ts, err := store.CreateBTreeStore(filename, sch)
	if err != nil {
		return nil, err
	}

	tableCache[filename] = ts
	return ts, nil
}

type DatabaseConfig struct {
	TableS *store.BTreeStore
}

type CliCommand struct {
	Name        string
	Description string
	Callback    func(*DatabaseConfig, []string, io.Writer) error
}

var CommandRegistry map[string]CliCommand

func init() {
	CommandRegistry = map[string]CliCommand{
		// add: UPDATE, DROP
		// future: SELECT command parsing for ranges, INSERT command PRIMARY KEY and NOT NULL
		//			CREATE database (right now it's just TABLE)
		".help": {
			Name:        ".help",
			Description: "Display available commands and usage information",
			Callback:    commandHelp,
		},
		".exit": {
			Name:        ".exit",
			Description: "Exit the database and close all connections",
			Callback:    commandExit,
		},
		"create": {
			Name:        "create",
			Description: "Create a new table - usage: create <tablename> <field:type> ...",
			Callback:    commandCreate,
		},
		"use": {
			Name:        "use",
			Description: "Switch to a different table - usage: use <tablename>",
			Callback:    commandUse,
		},
		"show": {
			Name:        "show",
			Description: "List all available tables in the database",
			Callback:    commandShow,
		},
		"insert": {
			Name:        "insert",
			Description: "Insert a new record into the active table - usage: insert <value1> <value2> ...",
			Callback:    commandInsert,
		},
		"select": {
			Name:        "select",
			Description: "Query records from the active table - usage: select [id] (omit id for full scan)",
			Callback:    commandSelect,
		},
		"delete": {
			Name:        "delete",
			Description: "Delete a record from the active table by primary key - usage: delete <id>",
			Callback:    commandDelete,
		},
		"stats": {
			Name:        "stats",
			Description: "Display B+ tree statistics for the active table (root page, type, page count)",
			Callback:    commandStats,
		},
	}
}

func commandStats(config *DatabaseConfig, params []string, w io.Writer) error {
	stats := config.TableS.Stats()
	fmt.Fprintln(w, stats)
	return nil
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

	newTableStore, err := store.CreateBTreeStore(fName, sch)
	if err != nil {
		return err
	}
	config.TableS = newTableStore
	fmt.Fprintf(w, "New table created: %s\n", newTableStore.Schema().TableName)
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

	// if the client is remote, just close the connection
	if conn, ok := w.(net.Conn); ok {
		return conn.Close()
	}

	// for local clients, close the entire db
	defer os.Exit(0)
	for _, v := range tableCache {
		err := v.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func commandDelete(config *DatabaseConfig, params []string, w io.Writer) error {

	if len(params) != 1 {
		return errors.New("must provide a primary key for deletion")
	}

	key, err := strconv.Atoi(params[0])
	if err != nil {
		return fmt.Errorf("error parsing primary key: %v", params[0])
	}
	record, err := config.TableS.Find(key)
	if err != nil {
		return fmt.Errorf("unable to find key: %d", key)
	}

	fmt.Fprintf(w, "Deleting %+v from table %s\n", record, config.TableS.Schema().TableName)
	return config.TableS.Delete(uint64(key))
}

func commandInsert(config *DatabaseConfig, params []string, w io.Writer) error {

	fieldCount := len(config.TableS.Schema().Fields)

	if len(params) != fieldCount {
		return fmt.Errorf("need %d parameters for fields: %v", fieldCount, config.TableS.Schema().GetFieldNames())
	}

	record := make(schema.Record)
	for i, field := range config.TableS.Schema().Fields {
		// parse params[i] according to field.type
		value, err := schema.ParseValue(params[i], field.Type)
		if err != nil {
			return fmt.Errorf("invalid value for %s: %v", field.Name, err)
		}
		record[field.Name] = value
	}
	fmt.Fprintf(w, "Inserting %+v into table %s\n", record, config.TableS.Schema().TableName)
	return config.TableS.Insert(record)
}

func selectAll(config *DatabaseConfig, w io.Writer) error {
	records, err := config.TableS.ScanAll()
	if err != nil {
		return err
	}

	fieldNames := config.TableS.Schema().GetFieldNames()
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
		for i, field := range config.TableS.Schema().Fields {
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

	fieldNames := config.TableS.Schema().GetFieldNames()
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

	key, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}

	record, err := config.TableS.Find(key)

	fmt.Fprint(w, "| ")
	for i, field := range config.TableS.Schema().Fields {
		val := fmt.Sprintf("%v", record[field.Name])
		fmt.Fprintf(w, "%-*s | ", widths[i], val)
	}
	fmt.Fprintln(w)

	return err
}
