package main

import (
	"io"
	"os"
	"os/exec"
	"strings"
)

// Cmd describes one external command a task runs.
type Cmd struct {
	// Name is the program to run; it is resolved on PATH by the runner.
	Name string
	// Args are the program arguments, without the program name.
	Args []string
	// Dir is the working directory. Tasks always set it to the module root.
	Dir string
	// Env holds extra KEY=VALUE pairs appended to the inherited environment.
	Env []string
	// Stdout receives the standard output of the process when non-nil.
	// When nil the runner's own stdout is used.
	Stdout io.Writer
}

// String renders the command the way a shell user would type it. Arguments
// containing whitespace are quoted so the rendering is unambiguous.
func (c Cmd) String() string {
	parts := make([]string, 0, len(c.Args)+1)
	parts = append(parts, c.Name)
	for _, a := range c.Args {
		if strings.ContainsAny(a, " \t\"'") {
			a = `"` + strings.ReplaceAll(a, `"`, `\"`) + `"`
		}
		parts = append(parts, a)
	}
	return strings.Join(parts, " ")
}

// Runner executes external commands. Tasks never touch os/exec directly so
// tests can substitute a fake runner and assert the exact commands built.
type Runner interface {
	// Run executes cmd, streaming its output, and returns its exit error.
	Run(cmd Cmd) error
	// Output executes cmd and returns its captured standard output. Standard
	// error is streamed.
	Output(cmd Cmd) ([]byte, error)
	// LookPath reports the absolute path of an executable, or an error when
	// it is not installed.
	LookPath(name string) (string, error)
}

// execRunner is the Runner backed by os/exec.
type execRunner struct {
	stdout io.Writer
	stderr io.Writer
}

func (r execRunner) build(c Cmd) *exec.Cmd {
	cmd := exec.Command(c.Name, c.Args...)
	cmd.Dir = c.Dir
	cmd.Env = append(os.Environ(), c.Env...)
	cmd.Stderr = r.stderr
	return cmd
}

// Run implements Runner.
func (r execRunner) Run(c Cmd) error {
	cmd := r.build(c)
	cmd.Stdout = r.stdout
	if c.Stdout != nil {
		cmd.Stdout = c.Stdout
	}
	return cmd.Run()
}

// Output implements Runner.
func (r execRunner) Output(c Cmd) ([]byte, error) {
	cmd := r.build(c)
	return cmd.Output()
}

// LookPath implements Runner.
func (r execRunner) LookPath(name string) (string, error) {
	return exec.LookPath(name)
}
