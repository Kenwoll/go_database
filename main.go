package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"unsafe"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS              MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS                PrepareResult = iota
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS	   ExecuteResult = iota
	EXECUTE_TABLE_FULL
	EXECUTE_UNRECOGNIZED_STATEMENT
)

const (
	TABLE_MAX_PAGES = 100
	PAGE_SIZE = 4096
	ROW_SIZE = unsafe.Sizeof(Row{})
	ROWS_PER_PAGE = int(PAGE_SIZE / ROW_SIZE)
	TABLE_FULL_SIZE = int(ROWS_PER_PAGE * TABLE_MAX_PAGES)
) 

type Statement struct {
	Type StatementType
	Row_to_insert Row
}

type Row struct {
	ID       uint32
	Username [32]byte
	Email    [255]byte
}

type Page struct {
	Rows [ROWS_PER_PAGE]Row
}

type Table struct {
	NumRows int
	Pages   [TABLE_MAX_PAGES]*Page
}


func printPrompt() {
	fmt.Print("db > ")
}

func printRow(row *Row) {
	fmt.Printf("(%d, %s, %s)\n", row.ID, row.Username, row.Email)
}

func readInput(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	input = strings.TrimSpace(input)

	return input, nil
}

func rowSlot(table *Table, rowNum int) *Row {
	pageNum := rowNum / ROWS_PER_PAGE
	rowOffset := rowNum % ROWS_PER_PAGE

	// Check if the page exists
	if pageNum >= len(table.Pages) || table.Pages[pageNum] == nil {
		// Allocate memory only when we try to access the page
		table.Pages[pageNum] = &Page{}
	}

	return &table.Pages[pageNum].Rows[rowOffset]
}

func create_Table() *Table {
	table := &Table{}
	table.NumRows = 0
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		table.Pages[i] = nil
	}

	return table
}

func executeMetaCommand(input string) MetaCommandResult {
	if strings.Compare(input, ".exit") == 0 {
		os.Exit(0)
		return META_COMMAND_SUCCESS
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND
	} 
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if strings.HasPrefix(input, "insert") {
		statement.Type = STATEMENT_INSERT

		var username, email string

		args, err := fmt.Sscanf(input, "insert %d %s %s", &statement.Row_to_insert.ID, &username, &email)
		if err != nil {
			return PREPARE_SYNTAX_ERROR
		}

		if args < 3 {
			return PREPARE_SYNTAX_ERROR
		}

		copy(statement.Row_to_insert.Username[:], username)
    	copy(statement.Row_to_insert.Email[:], email)

		return PREPARE_SUCCESS
	}
	if strings.Compare(input, "select") == 0 {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func execute_insert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TABLE_FULL_SIZE {
		return EXECUTE_TABLE_FULL
	}

	row := &statement.Row_to_insert
	rowSlot := rowSlot(table, table.NumRows)
	rowSlot.ID = row.ID
	copy(rowSlot.Username[:], row.Username[:])
	copy(rowSlot.Email[:], row.Email[:])
	table.NumRows += 1

	return EXECUTE_SUCCESS
}

func execute_select(statement *Statement, table *Table) ExecuteResult {
	row := Row{}
	for i := 0; i < table.NumRows; i++ {
		row = *rowSlot(table, i)
		printRow(&row)
	}

	return EXECUTE_SUCCESS
}

func executeStatement(Statement *Statement, table *Table) ExecuteResult {
	switch Statement.Type {
	case STATEMENT_SELECT:
		return execute_select(Statement, table)
	case STATEMENT_INSERT:
		return execute_insert(Statement, table)
	default:
		return EXECUTE_UNRECOGNIZED_STATEMENT
	}
}

func main() {

	// added for batch testing
	// pipePath := "mypipe"
	// pipe, err := os.OpenFile(pipePath, os.O_RDONLY, os.ModeNamedPipe)
	// if err != nil {
	// 	fmt.Println("Error opening pipe:", err)
	// 	return
	// }
	// defer pipe.Close()


	table := create_Table()
	reader := bufio.NewReader(os.Stdin)
	
	for {
		printPrompt()
		input, err := readInput(reader)
		if err != nil {
			fmt.Println("Error reading input:", err)
			os.Exit(1)
		}

		if strings.HasPrefix(input, ".") {
			switch executeMetaCommand(input) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'\n", input);
		        continue;
			}
		}

		var statement Statement
		switch prepareStatement(input, &statement) {
		case PREPARE_SUCCESS:
			break
		case PREPARE_SYNTAX_ERROR:
			fmt.Printf("Syntax error. Could not parse command\n")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized command '%s'\n", input)
			continue
		}
	
		
		switch executeStatement(&statement, table) {
		case EXECUTE_SUCCESS:
			fmt.Printf("Executed.\n")
			break
		case EXECUTE_TABLE_FULL:
			fmt.Printf("Table is full.\n")
			break
		}
	}
}