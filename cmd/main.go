package main

import (
	"bufio"
	"context"
	"fmt"
	"godb/internal/cli"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

func cleanInput(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

func ProcessCommand(input string, config *cli.DatabaseConfig, w io.Writer) error {
	cleanLine := cleanInput(input)
	if len(cleanLine) == 0 {
		return nil
	}
	cmd, ok := cli.CommandRegistry[cleanLine[0]]
	if !ok {
		return fmt.Errorf("unknown command")
	}
	return cmd.Callback(config, cleanLine[1:], w)
}

// RunREPL runs the interactive prompt.
// Note: scanner.Scan() blocks on stdin and doesn't respect context cancellation.
// On shutdown (Ctrl+C), checkpointers exit cleanly but prompt remains until Enter pressed.
func RunREPL(config *cli.DatabaseConfig) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("Go-DB [%s]> ", config.TableS.Schema().TableName)
		scanner.Scan()
		err := ProcessCommand(scanner.Text(), config, os.Stdout)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

func handleTCPConnection(conn net.Conn, baseConfig *cli.DatabaseConfig) {
	defer conn.Close()
	log.Printf("Client connected: %s", conn.RemoteAddr().String())

	sessionConfig := baseConfig.Clone()

	writer := bufio.NewWriter(conn)
	scanner := bufio.NewScanner(conn)

	fmt.Fprintf(writer, "Go-DB [%s]> ", sessionConfig.TableS.Schema().TableName)
	_ = writer.Flush()
	for scanner.Scan() {
		input := scanner.Text()
		log.Printf("Received: %s", input)

		err := ProcessCommand(input, sessionConfig, conn)
		if err != nil {
			fmt.Fprintf(conn, "error: %v\n", err)
		}

		// send prompt for next command
		fmt.Fprintf(writer, "\nGo-DB [%s]> ", sessionConfig.TableS.Schema().TableName)
		writer.Flush()
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error: %v", err)
	}
	log.Printf("Client disconnected: %s", conn.RemoteAddr().String())
}

func main() {
	log.SetOutput(os.Stderr)

	// create root context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup

	ts, err := cli.GetOrOpenTable("table.db", ctx, &wg)
	if err != nil {
		log.Fatal(err)
	}

	config := cli.NewDatabaseConfig(ts, ctx, &wg)

	go func() {
		<-sigCh
		fmt.Println("\nShutting down gracefully...")
		cancel()
		wg.Wait()
		_ = cli.CommandRegistry[".exit"].Callback(config, []string{}, os.Stdout)
		os.Exit(0)
	}()

	go func() {
		listener, err := net.Listen("tcp", ":42069")
		if err != nil {
			log.Printf("TCP server failed: %v", err)
		}
		defer listener.Close()

		log.Printf("TCP server listening on %v\n", listener.Addr().String())

		// channel for accepted connections
		connChan := make(chan net.Conn)

		// goroutine that accepts connections
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					// listener closed, stop accepting
					close(connChan)
					return
				}
				connChan <- conn
			}
		}()

		// main loop: select between context and connections
		for {
			select {
			case <-ctx.Done():
				// context cancelled, close listener and exit
				listener.Close()
				return
			case conn, ok := <-connChan:
				if !ok {
					// channel closed (listener error), exit
					return
				}
				go handleTCPConnection(conn, config)
			}
		}
	}()

	RunREPL(config)
}
