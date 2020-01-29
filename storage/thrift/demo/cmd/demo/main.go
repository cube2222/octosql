package main

import (
    "flag"
    "fmt"
    "github.com/apache/thrift/lib/go/thrift"
    "github.com/cube2222/octosql/storage/thrift/demo/client"
    "github.com/cube2222/octosql/storage/thrift/demo/server"
    "os"
)

// Print usage
func Usage() {
    fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
    flag.PrintDefaults()
    fmt.Fprint(os.Stderr, "\n")
}

func main() {
    flag.Usage = Usage
    protocol := flag.String("P", "binary", "Specify the protocol (binary, compact, json, simplejson)")
    framed := flag.Bool("framed", false, "Use framed transport")
    buffered := flag.Bool("buffered", false, "Use buffered transport")
    addr := flag.String("addr", "localhost:9090", "Address to listen to")
    secure := flag.Bool("secure", false, "Use tls secure transport")
    serverMode := flag.Bool("server", false, "Run in server mode")

    flag.Parse()

    var protocolFactory thrift.TProtocolFactory
    switch *protocol {
    case "compact":
        protocolFactory = thrift.NewTCompactProtocolFactory()
    case "simplejson":
        protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
    case "json":
        protocolFactory = thrift.NewTJSONProtocolFactory()
    case "binary", "":
        protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
    default:
        fmt.Fprint(os.Stderr, "Invalid protocol specified", protocol, "\n")
        Usage()
        os.Exit(1)
    }

    var transportFactory thrift.TTransportFactory
    if *buffered {
        transportFactory = thrift.NewTBufferedTransportFactory(8192)
    } else {
        transportFactory = thrift.NewTTransportFactory()
    }

    if *framed {
        transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
    }

    if *serverMode {
        // Always run server here
        if err := server.RunServer(transportFactory, protocolFactory, *addr, *secure); err != nil {
            fmt.Println("error running server:", err)
        }
    } else {
        // Always be client
        fmt.Printf("*secure = '%v'\n", *secure)
        fmt.Printf("*addr   = '%v'\n", *addr)
        if err := client.RunClient(transportFactory, protocolFactory, *addr, *secure); err != nil {
            fmt.Println("error running client:", err)
        }
    }
}