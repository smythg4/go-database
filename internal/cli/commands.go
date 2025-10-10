package cli

import (
	"errors"
	"fmt"
	"godb/internal/schema"
	"godb/internal/store"
	"io"
	"math"
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
		".help": {
			Name:        ".help",
			Description: "Show all available commands",
			Callback:    commandHelp,
		},
		".exit": {
			Name:        ".exit",
			Description: "Exit the database",
			Callback:    commandExit,
		},
		"create": {
			Name:        "create",
			Description: "Create new table - usage: create <table> <field:type> ... (first field is primary key)",
			Callback:    commandCreate,
		},
		"use": {
			Name:        "use",
			Description: "Switch active table - usage: use <table>",
			Callback:    commandUse,
		},
		"show": {
			Name:        "show",
			Description: "List all tables",
			Callback:    commandShow,
		},
		"describe": {
			Name:        "describe",
			Description: "Show schema for active table",
			Callback:    commandDescribe,
		},
		"insert": {
			Name:        "insert",
			Description: "Insert record - usage: insert <val1> <val2> ... (must match schema)",
			Callback:    commandInsert,
		},
		"select": {
			Name:        "select",
			Description: "Query records - usage: select | select <id> | select <start> <end>",
			Callback:    commandSelect,
		},
		"update": {
			Name:        "update",
			Description: "Update record - usage: update <val1> <val2> ... (primary key must exist)",
			Callback:    commandUpdate,
		},
		"delete": {
			Name:        "delete",
			Description: "Delete record - usage: delete <id>",
			Callback:    commandDelete,
		},
		"count": {
			Name:        "count",
			Description: "Count records - usage: count | count <id> | count <start> <end>",
			Callback:    commandCount,
		},
		"stats": {
			Name:        "stats",
			Description: "Show B+ tree statistics (root page, type, page count)",
			Callback:    commandStats,
		},
		"drop": {
			Name:        "drop",
			Description: "Delete the underlying table - usage: drop | <tablename>",
			Callback:    commandDrop,
		},
		"vacuum": {
			Name:        "vacuum",
			Description: "Systematic compaction and orphan page reaping",
			Callback:    commandVacuum,
		},
	}
}

func commandVacuum(config *DatabaseConfig, params []string, w io.Writer) error {
	fmt.Fprintf(w, "Vacuuming up table %s...\n", config.TableS.Schema().TableName)

	err := config.TableS.Vacuum() // save error for future use so you can always refresh the table cache

	tableName := config.TableS.Schema().TableName
	fName := tableName + ".db"

	// Clear cache and reload fresh instance
	tableCacheMu.Lock()
	delete(tableCache, fName)
	tableCacheMu.Unlock()

	// Reload the table fresh from disk
	freshTable, reloadErr := GetOrOpenTable(fName)
	if reloadErr != nil {
		return fmt.Errorf("failed to reload after vacuum: %v", reloadErr)
	}
	config.TableS = freshTable

	if err != nil {
		return fmt.Errorf("vacuum failed: %v", err)
	}

	fmt.Fprintf(w, "Vacuum complete.\n")
	return nil
}

func fieldString(typ schema.FieldType) (string, error) {
	switch typ {
	case schema.IntType:
		return "int", nil
	case schema.FloatType:
		return "float", nil
	case schema.StringType:
		return "string", nil
	case schema.BoolType:
		return "bool", nil
	case schema.DateType:
		return "date", nil
	default:
		return "", fmt.Errorf("type not found: %v", typ)
	}

}

func commandDrop(config *DatabaseConfig, params []string, w io.Writer) error {
	if len(params) != 1 {
		return fmt.Errorf("need at least one parameter, actual: %d", len(params))
	}
	tName := params[0]
	fName := tName + ".db"
	fmt.Fprintf(w, "Dropping table %s...\n", tName)
	err := os.Remove(fName)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	tableCacheMu.Lock()
	delete(tableCache, fName)
	tableCacheMu.Unlock()

	fmt.Fprintf(w, "Dropped table %s\n", tName)
	return nil
}

func commandDescribe(config *DatabaseConfig, params []string, w io.Writer) error {
	var pKeyHuh string
	sch := config.TableS.Schema()
	tName := sch.TableName
	fmt.Fprintf(w, "Table: %s\n", tName)
	for i, rec := range sch.Fields {
		fName := rec.Name
		fType, err := fieldString(rec.Type)
		if err != nil {
			return err
		}
		if i == 0 {
			pKeyHuh = " - PRIMARY KEY"
		} else {
			pKeyHuh = ""
		}
		fmt.Fprintf(w, "   %s (%s)%s\n", fName, fType, pKeyHuh)
	}
	return nil
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

	// // if the client is remote, just close the connection
	// for name, v := range tableCache {
	// 	fmt.Printf("DEBUG: Closing table %s\n", name)
	// 	if err := v.Close(); err != nil {
	// 		fmt.Printf("Error closing %s: %v\n", name, err)
	// 		//return err
	// 	}
	// }
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

func commandUpdate(config *DatabaseConfig, params []string, w io.Writer) error {
	// this is a naive implementation of UPDATE. It just DELETES then INSERTS.
	// we can make a true mutable UPDATE later.
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
	fmt.Fprintf(w, "Updating %+v in table %s\n", record, config.TableS.Schema().TableName)
	firstField := config.TableS.Schema().Fields[0].Name
	key := record[firstField].(int32)
	err := config.TableS.Delete(uint64(key))
	if err != nil {
		return err
	}
	return config.TableS.Insert(record)
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

func rangeScan(config *DatabaseConfig, w io.Writer, params []string) error {
	startKey, err := strconv.Atoi(params[0])
	if err != nil {
		return err
	}
	endKey, err := strconv.Atoi(params[1])
	if err != nil {
		return err
	}

	records, err := config.TableS.RangeScan(uint64(startKey), uint64(endKey))
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

	if len(params) == 2 {
		return rangeScan(config, w, params)
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

func commandCount(config *DatabaseConfig, params []string, w io.Writer) error {
	var startKey uint64
	var endKey uint64
	var err error
	if len(params) == 0 {
		startKey = 0
		endKey = math.MaxUint64
	}

	if len(params) == 2 {
		sk, err := strconv.Atoi(params[0])
		if err != nil {
			return err
		}
		ek, err := strconv.Atoi(params[1])
		if err != nil {
			return err
		}
		startKey = uint64(sk)
		endKey = uint64(ek)
	}

	if len(params) == 1 {
		sk, err := strconv.Atoi(params[0])
		if err != nil {
			return err
		}
		startKey = uint64(sk)
		endKey = uint64(sk)
	}
	records, err := config.TableS.RangeScan(startKey, endKey)
	if err != nil {
		return err
	}
	count := len(records)
	fmt.Fprintf(w, "Count: %d\n", count)
	return nil
}
