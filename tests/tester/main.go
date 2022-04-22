package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/pmezard/go-difflib/difflib"
)

func completer(d prompt.Document) []prompt.Suggest {
	return nil
}

func handleError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	if err := os.Chdir("tests/scenarios"); err != nil {
		if os.IsNotExist(err) {
			fmt.Println("tests/scenarios directory does not exist, please run the tester in the root of the project")
			os.Exit(1)
		}
		handleError(err)
	}
	if len(os.Args) > 1 && os.Args[1] == "ci" {
		ok := true
		testCases := loadTestCases()
		for _, testCase := range testCases {
			fmt.Println(testCase)
			runTest(testCase)
			if diffTest(testCase, true) {
				ok = false
			}
			cleanupTest(testCase)
		}
		if ok {
			return
		} else {
			os.Exit(1)
		}
	}
	fmt.Println("list <pattern>, run <pattern>, update <pattern>, exit")
	prompt.New(executeCommand, completer).Run()
}

func executeCommand(command string) {
	switch {
	case command == "list":
		command = "list *"
		fallthrough
	case regexp.MustCompile(`^list [^ ]+$`).FindStringSubmatch(command) != nil:
		pattern := regexp.MustCompile(`^list ([^ ]+)$`).FindStringSubmatch(command)[1]
		parsedPattern := strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", "[^ ]+")
		re := regexp.MustCompile(parsedPattern)
		testCases := loadTestCases()
		for _, testCase := range testCases {
			if !re.MatchString(testCase) {
				continue
			}
			fmt.Println(testCase)
		}
	case regexp.MustCompile(`^run [^ ]+$`).FindStringSubmatch(command) != nil:
		pattern := regexp.MustCompile(`^run ([^ ]+)$`).FindStringSubmatch(command)[1]
		parsedPattern := strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", "[^ ]+")
		re := regexp.MustCompile(parsedPattern)
		testCases := loadTestCases()
		for _, testCase := range testCases {
			if !re.MatchString(testCase) {
				continue
			}
			fmt.Println(testCase)
			runTest(testCase)
			isDiff := diffTest(testCase, true)
			if isDiff {
				fmt.Println("Diff found, temporary output files left in place.")
			} else {
				cleanupTest(testCase)
			}
		}
	case regexp.MustCompile(`^update [^ ]+$`).FindStringSubmatch(command) != nil:
		pattern := regexp.MustCompile(`^update ([^ ]+)$`).FindStringSubmatch(command)[1]
		parsedPattern := strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", "[^ ]+")
		re := regexp.MustCompile(parsedPattern)
		testCases := loadTestCases()
		for _, testCase := range testCases {
			if !re.MatchString(testCase) {
				continue
			}
			fmt.Println(testCase)
			runTest(testCase)
			isDiff := diffTest(testCase, false)
			if isDiff {
				fmt.Println("Diff found, updating...")
				updateTest(testCase)
			}
			cleanupTest(testCase)
		}
	case command == "exit":
		fmt.Println("Exiting.")
		return
	default:
		fmt.Println("Unknown command.")
		fmt.Println("list <pattern>, run <pattern>, update <pattern>, exit")
	}
}

func runTest(testCase string) {
	body, err := os.ReadFile(testCase + ".in")
	handleError(err)
	testCommand := strings.TrimSpace(string(body))

	cmd := exec.Command("bash", "-c", testCommand+fmt.Sprintf(" > %s 2> %s", filepath.Base(testCase)+".tmpout", filepath.Base(testCase)+".tmperr"))
	cmd.Env = append(os.Environ(), "OCTOSQL_NO_TELEMETRY=1")
	cmd.Dir = filepath.Dir(testCase)
	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// That's fine
		} else {
			handleError(err)
		}
	}
}

func diffTest(testCase string, print bool) bool {
	diff := false

	stdoutDiff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(readFileOrEmpty(testCase + ".out")),
		B:        difflib.SplitLines(readFileOrEmpty(testCase + ".tmpout")),
		FromFile: "Expected Standard Output",
		ToFile:   "Actual Standard Output",
		Context:  2,
	})
	if stdoutDiff != "" {
		if print {
			fmt.Println(stdoutDiff)
		}
		diff = true
	}

	stderrDiff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(readFileOrEmpty(testCase + ".err")),
		B:        difflib.SplitLines(readFileOrEmpty(testCase + ".tmperr")),
		FromFile: "Expected Standard Error",
		ToFile:   "Actual Standard Error",
		Context:  2,
	})
	if stderrDiff != "" {
		if print {
			fmt.Println(stderrDiff)
		}
		diff = true
	}
	return diff
}

func updateTest(testCase string) {
	handleError(os.Rename(testCase+".tmpout", testCase+".out"))
	handleError(os.Rename(testCase+".tmperr", testCase+".err"))
}

func cleanupTest(testCase string) {
	handleError(os.RemoveAll(testCase + ".tmpout"))
	handleError(os.RemoveAll(testCase + ".tmperr"))
}

func readFileOrEmpty(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		handleError(err)
	}
	return string(data)
}

func loadTestCases() []string {
	var testCases []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if filepath.Ext(path) != ".in" {
			return nil
		}
		testCases = append(testCases, path[:len(path)-3])
		return nil
	})
	handleError(err)

	return testCases
}
