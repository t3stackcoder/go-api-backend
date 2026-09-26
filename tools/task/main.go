// Command task implements every Makefile target of spec 12.2 so nothing
// depends on make being installed:
//
//	go run ./tools/task <task> [args]
//
// The Makefile is a thin alias over this command. It has no dependencies
// outside the standard library. Every task builds its external commands
// through the Runner interface, which the tests replace with a fake to
// assert the exact commands each target constructs. Commands always run
// from the module root, found by walking up from the working directory to
// the nearest go.mod.
//
// Exit codes: 0 success, 1 the task failed, 2 usage error.
package main

import (
	"fmt"
	"os"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	app := &App{
		Runner: execRunner{stdout: os.Stdout, stderr: os.Stderr},
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Root:   findRoot(cwd),
	}
	os.Exit(app.Run(os.Args[1:]))
}
