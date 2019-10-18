package server

import (
	"crypto/tls"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cube2222/octosql/storage/thrift/demo/gen-go/model"
)

func RunServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string, secure bool) error {
	var transport thrift.TServerTransport
	var err error
	if secure {
		cfg := new(tls.Config)
		if cert, err := tls.LoadX509KeyPair("keys/server.crt", "keys/server.key"); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		} else {
			return err
		}
		transport, err = thrift.NewTSSLServerSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTServerSocket(addr)
	}

	if err != nil {
		return err
	}
	fmt.Printf("%T\n", transport)
	handler := NewCalculatorHandler()
	processor := model.NewImageServiceProcessor(handler)

	handler.serverLiveWG.Add(1)
	var server *thrift.TSimpleServer = thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	go (func () {
		handler.serverLiveWG.Wait()
		fmt.Println("Stopping server...")
		err = server.Stop()
		if err != nil {
			fmt.Println(err)
		}
	})()

	fmt.Println("Starting the simple server... on ", addr)
	return server.Serve()
}