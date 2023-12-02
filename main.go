package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS              MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED_COMMAND
)

type PrepareResult int

const (
	PREPARE_SUCCESS                PrepareResult = iota
	PREPARE_UNRECOGNIZED_STATEMENT
)

type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

type Statement struct {
	Type StatementType
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	input = strings.TrimSpace(input)

	return input, nil
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
		return PREPARE_SUCCESS
	}
	if strings.Compare(input, "select") == 0 {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(Statement *Statement) {
	switch Statement.Type {
	case STATEMENT_SELECT:
		fmt.Println("This is were we will apply select")
		break
	case STATEMENT_INSERT:
		fmt.Println("This is were we will apply insert")
		break
	}
}

func main() {
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
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized command '%s'\n", input)
			continue
		}
	
		
		executeStatement(&statement);
	    fmt.Printf("Executed.\n")
	
	}
}