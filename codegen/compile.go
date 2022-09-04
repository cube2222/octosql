package codegen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/cube2222/octosql/physical"
)

func CompileAndRun(ctx context.Context, node physical.Node) {
	body := Generate(node)
	os.RemoveAll("codegen/tmp/code")
	dirName := fmt.Sprintf("codegen/tmp/code") // %d", time.Now().UnixNano())
	if err := os.Mkdir(dirName, 0777); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dirName, "main.go"), []byte(body), 0777); err != nil {
		panic(err)
	}
	buildCommand := exec.Command("go", "build", "-o", "main")
	buildCommand.Stdout = os.Stdout
	buildCommand.Stderr = os.Stderr
	buildCommand.Dir = dirName
	if err := buildCommand.Run(); err != nil {
		panic(err)
	}
	cmd := exec.Command(filepath.Join(dirName, "main"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}
