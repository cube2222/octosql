//
// Utilities for the demo client
//
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql/storage/thrift/demo/gen-go/model"
)

type ClientFn func(client *model.ImageServiceClient) error

var defaultCtx = context.Background()

func handleClient(client *model.ImageServiceClient) (err error) {
	client.Ping(defaultCtx)
	client.Close(defaultCtx)
	return err
}

func handleClientStopServer(client *model.ImageServiceClient) (err error) {
	client.Close(defaultCtx)
	return err
}

func RunClientStopServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	return RunClientFn(transportFactory, protocolFactory, addr, secure, handleClientStopServer)
}

func RunClient(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	return RunClientFn(transportFactory, protocolFactory, addr, secure, handleClient)
}

func RunClientFn(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool, clientFn ClientFn) error {
	var transport thrift.TTransport
	var err error
	if secure {
		cfg := new(tls.Config)
		cfg.InsecureSkipVerify = true
		transport, err = thrift.NewTSSLSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTSocket(addr)
	}
	if err != nil {
		fmt.Println("Error opening socket:", err)
		return err
	}
	if transport == nil {
		return fmt.Errorf("Error opening socket, got nil transport. Is server available?")
	}
	transport, _ = transportFactory.GetTransport(transport)
	if transport == nil {
		return fmt.Errorf("Error from transportFactory.GetTransport(), got nil transport. Is server available?")
	}

	err = transport.Open()
	if err != nil {
		return err
	}
	defer transport.Close()

	return clientFn(model.NewImageServiceClientFactory(transport, protocolFactory))
}