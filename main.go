package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/cube2222/octosql/cmd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		cancel()
		<-signals
		fmt.Println("Force stopped without cleanup.")
		os.Exit(1)
	}()

	cmd.Execute(ctx)
}
