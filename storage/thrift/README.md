# Thrift data source

This module provides support for Apache Thrift

## Usage

You must specify the following properties in `octosql.yaml`:
```yaml
  - name: lol
    type: thrift
    config:
      ip: "localhost:9090"
      protocol: "binary"
      secure: false
```

You can then connect via Apache Thrift to your server.
The server must implement the following methods on a service:
```
    service ImageService {
       i32 openRecords(),
       ReturnedRecord getRecord(1:i32 streamID),
    }
```

You can return any data.

## The demo server

The module comes with its own demo server.
The server has the following specification in IDL:
```
    struct Record {
        1: string a,
        2: string b,
    }
    
    service ImageService {
       void ping(),
    
       i32 openRecords(),
       Record getRecord(1:i32 streamID),
       void close(),
    }
```

The go static code was generated with `thrift -r --gen go model.thrift` command.
You can launch the server by simply calling:
```bash
    $ go run ./cmd/demo/main.go -server
```

Or demo client using:
```bash
    $ go run ./cmd/demo/main.go
```

## Tests

The tests automatically run Thrift server goroutine.
Additionaly `close()` method is implemented to shutdown the server.