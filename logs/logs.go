package logs

import (
	"log"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
)

var Output *os.File

func InitializeFileLogger() {
	dir, err := homedir.Dir()
	if err != nil {
		log.Fatalf("couldn't get user home directory: %s", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".octosql"), 0755); err != nil {
		log.Fatalf("couldn't create ~/.octosql home directory: %s", err)
	}
	path := filepath.Join(dir, ".octosql/logs.txt")
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("couldn't create logs file: %s", err)
	}
	Output = f
	log.SetOutput(Output)
}

func CloseLogger() {
	Output.Close()
}
