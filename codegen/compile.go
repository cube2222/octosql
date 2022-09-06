package codegen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"time"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
	"github.com/traefik/yaegi/stdlib/syscall"
	"github.com/traefik/yaegi/stdlib/unsafe"

	"github.com/cube2222/octosql/physical"
)

func CompileAndRun(ctx context.Context, node physical.Node) {
	start := time.Now()
	body := Generate(node)
	fmt.Printf("generation: %s\n", time.Since(start))

	compileAndRun(ctx, body)
}

func compileAndRun(ctx context.Context, body string) {
	os.RemoveAll("codegen/tmp/code")
	dirName := fmt.Sprintf("codegen/tmp/code") // %d", time.Now().UnixNano())
	if err := os.MkdirAll(dirName, 0777); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(dirName, "main.go"), []byte(body), 0777); err != nil {
		panic(err)
	}
	start := time.Now()

	buildCommand := exec.Command("go", "build", "-o", "main", "-debug-actiongraph=compile.json")
	buildCommand.Stdout = os.Stdout
	buildCommand.Stderr = os.Stderr
	buildCommand.Dir = dirName
	if err := buildCommand.Run(); err != nil {
		panic(err)
	}
	fmt.Printf("compilation: %s\n", time.Since(start))
	start = time.Now()

	cmd := exec.Command(filepath.Join(dirName, "main"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
	fmt.Printf("execution: %s\n", time.Since(start))
}

var Symbols = map[string]map[string]reflect.Value{}

//go:generate yaegi extract github.com/cube2222/octosql/codegen/lib
//go:generate yaegi extract github.com/cube2222/octosql/execution/files
//go:generate yaegi extract github.com/valyala/fastjson

func interpret(ctx context.Context, body string) {
	i := interp.New(interp.Options{
		GoPath:               "",
		BuildTags:            nil,
		Stdin:                nil,
		Stdout:               nil,
		Stderr:               nil,
		Args:                 nil,
		Env:                  nil,
		SourcecodeFilesystem: nil,
		Unrestricted:         true,
	})

	i.Use(stdlib.Symbols)
	i.Use(syscall.Symbols)
	i.Use(unsafe.Symbols)
	i.Use(Symbols)

	_, err := i.Eval(`import "fmt"`)
	if err != nil {
		panic(err)
	}
	res, err := i.Eval(body)
	fmt.Println(res, err)
}
