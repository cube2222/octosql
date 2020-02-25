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
    struct Foo {
        1: string fooString,
        2: i16 fooInt16,
        3: i32 fooInt32,
        4: i64 fooInt64,
        5: double fooDouble,
        6: byte fooByte,
        7: bool fooBool,
    }
    
    struct Bar {
        1: string barString,
    }
    
    struct Record {
        1: string a,
        3: optional Bar bar,
        4: required Foo foo,
        5: list<string> lst,
    }
    
    service ImageService {
       void ping(),
    
       i32 openRecords(),
       Record getRecord(1:i32 streamID),
       void close(),
    }
```

The go static code was generated with `thrift -r --gen go model.thrift` command
(please see gen-go directory to see the generated go code).

You can launch the server by simply calling:
```bash
    $ go run ./cmd/demo/main.go -server
```

Or demo client using:
```bash
    $ go run ./cmd/demo/main.go
```

If you launch the server you can query it supposing you have the following Octosql configuration:
```yaml
- name: lol
    type: thrift
    config:
      ip: "localhost:9090"
      protocol: "binary"
      secure: false
```

You can type the following query:
```bash
    $ go run cmd/octosql/main.go "SELECT * FROM lol lol"
```

## Thrift metadata

Please note that after running the query to the example server you got field names like `"<table>.field_0.field_1"`.
This is caused by the lack of the metadata inside Thrift payload.

To overcome that difficulty you can specify a path to the Thrift specification to get full names of the fields:
```yaml
dataSources:
  - name: lol
    type: thrift
    config:
      thriftSpecs: "./demo/model.thrift"
      ip: "localhost:9090"
      protocol: "binary"
      secure: false
```

In that case Thrift datasource will read the specs, parse it and map the received Thrift payload into 
the specification AST node, so you will be able to read dull fields names.

**Important note:** At the current stage the following features **ARE NOT** supported: 
- maps and sets
- includes (every struct definition required by the return type of the called method should be placed in a single file)
- cyclic optional fields when they're nulled (in this case program hangs)

## Tests

The tests automatically run Thrift server goroutine.
Additionaly `close()` method is implemented to shutdown the server.