package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// fuzzTarget is one Fuzz function found in the module.
type fuzzTarget struct {
	// Pkg is the package pattern relative to the module root, e.g. ./mediator/validate.
	Pkg string
	// Name is the function name, e.g. FuzzTagGrammar.
	Name string
}

// discoverFuzzTargets scans every *_test.go file under root that is part of
// the default build and returns the Fuzz functions it declares, sorted by
// package then name. Directories named testdata or vendor and those starting
// with "." or "_" are skipped.
func discoverFuzzTargets(root string) ([]fuzzTarget, error) {
	var targets []fuzzTarget
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") ||
				name == "testdata" || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		names, err := fuzzFuncs(path)
		if err != nil {
			return err
		}
		if len(names) == 0 {
			return nil
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := "."
		if rel != "." {
			pkg = "./" + filepath.ToSlash(rel)
		}
		for _, n := range names {
			targets = append(targets, fuzzTarget{Pkg: pkg, Name: n})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Pkg != targets[j].Pkg {
			return targets[i].Pkg < targets[j].Pkg
		}
		return targets[i].Name < targets[j].Name
	})
	return targets, nil
}

// fuzzFuncs returns the names of the top-level functions of the form
// `func FuzzX(f *testing.F)` in a test file, or nothing when the file is
// excluded from the default build by its //go:build constraint.
func fuzzFuncs(path string) ([]string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !buildsByDefault(src) {
		return nil, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	var names []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Fuzz") {
			continue
		}
		if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
			continue
		}
		if !isTestingF(fn.Type.Params.List[0].Type) {
			continue
		}
		names = append(names, fn.Name.Name)
	}
	return names, nil
}

// isTestingF reports whether e is the type expression *testing.F.
func isTestingF(e ast.Expr) bool {
	star, ok := e.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "testing" && sel.Sel.Name == "F"
}

// buildsByDefault evaluates the file's //go:build line, if any, against the
// host GOOS and GOARCH with no extra tags, so files tagged integration,
// faultsweep, or chaos are left to their own tiers.
func buildsByDefault(src []byte) bool {
	sc := bufio.NewScanner(bytes.NewReader(src))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "":
			continue
		case constraint.IsGoBuild(line):
			expr, err := constraint.Parse(line)
			if err != nil {
				return false
			}
			return expr.Eval(defaultTag)
		case strings.HasPrefix(line, "//"):
			continue
		}
		// The package clause or a block comment ends the constraint region.
		return true
	}
	return true
}

// defaultTag is the tag predicate of a plain `go test` invocation.
func defaultTag(tag string) bool {
	switch {
	case tag == runtime.GOOS || tag == runtime.GOARCH || tag == "gc":
		return true
	case tag == "unix":
		return runtime.GOOS != "windows" && runtime.GOOS != "plan9" && runtime.GOOS != "js" && runtime.GOOS != "wasip1"
	case strings.HasPrefix(tag, "go1."):
		return true
	}
	return false
}
