package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = "+"
	ERROR   = "-"
	INTEGER = ":"
	BULK    = "$"
	ARRAY   = "*"
)

type Value struct {
	Typ   string // type of value
	Str   string // simple string
	Num   int
	Bulk  string // bulk string
	Array []Value
}

type Writer struct {
	writer io.Writer
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	// Line was declared aboce®
	for {
		byt, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		n += 1
		line = append(line, byt)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()

	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.Typ = "array"
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.Array = make([]Value, 0)

	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.Array = append(v.Array, val)
	}

	return v, nil

}

func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.Typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.Bulk = string(bulk)
	r.readInteger() // trailing crlf (carrieage return )

	return v, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

	switch string(_type) {
	case ARRAY:
		return r.readArray()

	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}

}

// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
/*

Read first byte to determine the type
(type * - array)
Read second byte to determine the size
(2)

loop twice
	Read third/fifth byte for type
	(type $ - bulk)
	Read fourth/sixth for size
	(5)
*/

// Serialize
func (v Value) Marshal() []byte {
	switch v.Typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "error":
		return v.marshalError()
	case "null":
		return v.marshalNull()

	default:
		return []byte{}
	}
}

func (v Value) marshalArray() []byte {
	len := len(v.Array)
	var bytes []byte
	bytes = append(bytes, []byte(ARRAY)...)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, []byte(BULK)...)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes

}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, []byte(STRING)...)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, []byte(ERROR)...)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")

}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
