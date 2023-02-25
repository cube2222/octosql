package logs

import (
	"log"
	"os"
	"path/filepath"

	"github.com/cube2222/octosql/config"
)

var Output *os.File

func InitializeFileLogger() {
	path := filepath.Join(config.OctosqlCacheDir, "logs.txt")
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
