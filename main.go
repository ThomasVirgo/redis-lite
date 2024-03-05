package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

var key_value_store sync.Map

func main() {
	port := 6379
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server is listening on port %d", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		} else {
			fmt.Println("\nclient connected")
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		fmt.Println("Closing connection", conn)
		conn.Close()
	}()
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("failed to read into buffer")
		return
	}
	command_args, err := deserialize(string(buffer[:n]))
	if err != nil {
		fmt.Println("Error: ", err)
	}
	process_command(command_args, conn)
}

func deserialize(s string) ([]string, error) {
	request_parts := strings.Split(s, "\r\n")
	request_parts = request_parts[:len(request_parts)-1]
	if len(request_parts) < 1 {
		return []string{}, errors.New("empty request")
	}
	if request_parts[0][0] != '*' {
		return []string{}, errors.New("expected request to be array type")
	}
	var command_args []string
	for _, value := range request_parts {
		if value[0] == '*' || value[0] == '$' {
			continue
		}
		command_args = append(command_args, value)
	}
	return command_args, nil
}

func process_command(command_args []string, conn net.Conn) {
	first_command := strings.ToUpper(command_args[0])
	switch first_command {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		word_to_echo := command_args[1]
		conn.Write(serializeString(word_to_echo))
	case "GET":
		value, ok := key_value_store.Load(command_args[1])
		if !ok {
			conn.Write([]byte("$-1\r\n"))
		}
		if v, ok := value.(int); ok {
			conn.Write(serializeInteger(v))
		}
		if v, ok := value.(string); ok {
			conn.Write(serializeString(v))
		}

	case "SET":
		key := command_args[1]
		value := command_args[2]
		if value[0] == ':' {
			value_int, err := strconv.Atoi(value[1:])
			if err != nil {
				fmt.Println("failed to convert value to integer", value)
			}
			key_value_store.Store(key, value_int)
		} else {
			key_value_store.Store(key, value)
		}
		conn.Write([]byte("+OK\r\n"))
	}
}

func serializeString(s string) []byte {
	serialized_string := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	return []byte(serialized_string)
}

func serializeInteger(i int) []byte {
	serialized_string := fmt.Sprintf(":%d", i)
	return []byte(serialized_string)
}
