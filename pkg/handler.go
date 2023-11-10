package resp

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	return Value{Typ: "string", Str: "PONG"}
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{Typ: "error", Str: "Err wrong number of arguments for 'set' command"}
	}
	key := args[0].Bulk
	value := args[1].Bulk

	// to handle concurrency.
	// data is not modifued by multiple threads at the same time.
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{Typ: "string", Str: "Ok"}
}

func get(args []Value) Value {

	if len(args) != 1 {
		return Value{Typ: "error", Str: "Err wrong numer of arguments for 'get' commond"}
	}

	key := args[0].Bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{Typ: "error", Str: "Err wrong number of arguments for 'hset' command"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	// to handle concurrency.
	// data is not modifued by multiple threads at the same time.
	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{Typ: "string", Str: "Ok"}
}

func hget(args []Value) Value {

	if len(args) != 2 {
		return Value{Typ: "error", Str: "Err wrong numer of arguments for 'hget' commond"}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]

	HSETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: value}
}

func hgetall(args []Value) Value {

	if len(args) != 1 {
		return Value{Typ: "error", Str: "Err wrong numer of arguments for 'hgetall' commond"}
	}

	hash := args[0].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{Typ: "bulk", Bulk: k})
		values = append(values, Value{Typ: "bulk", Bulk: v})
	}

	return Value{Typ: "array", Array: values}
}
