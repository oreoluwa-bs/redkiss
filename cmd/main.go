package main

import (
	"fmt"
	"net"
	"strings"

	resp "github.com/oreoluwa-bs/redkiss/pkg"
)

func main() {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Listening on port :6379")

	aof, err := resp.NewAof("database.aof")

	if err != nil {
		fmt.Println(err)
	}

	defer aof.Close()

	aof.Read(func(value resp.Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		handler, ok := resp.Handlers[command]

		if !ok {
			fmt.Println("Invalid  command: ", command)
			return
		}

		handler(args)
	})

	connection, err := listener.Accept()

	if err != nil {
		fmt.Println(err)
		return
	}

	defer connection.Close()

	for {
		// buffer := make([]byte, 1024)

		// // Read message from client and store in buffer
		// // array of bytes with a length of 1024
		// _, err = connection.Read(buffer)
		// if err != nil {
		// 	if err == io.EOF {
		// 		break
		// 	}

		// 	fmt.Println("error reading from client: ", err.Error())
		// 	os.Exit(1)
		// }

		_resp := resp.NewResp(connection)
		value, err := _resp.Read()

		if err != nil {
			fmt.Println(err)
			return
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Typ) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(connection)

		handler, ok := resp.Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(resp.Value{Typ: "string", Str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}
