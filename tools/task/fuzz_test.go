package main

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestDiscoverFuzzTargets(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "go.mod"), "module example.com/m\n\ngo 1.27\n")
	// Two targets in one package, plus a method and a non-F parameter to ignore.
	writeFile(t, filepath.Join(root, "mediator", "validate", "tag_test.go"), `package validate

import "testing"

func FuzzTagGrammar(f *testing.F) {}

func FuzzAlso(f *testing.F) {}

type x struct{}

func (x) FuzzMethod(f *testing.F) {}

func FuzzNotReally(t *testing.T) {}

func FuzzTwoParams(f *testing.F, extra int) {}

func Fuzz(f *testing.F) {}
`)
	// A tagged file is not part of the default build.
	writeFile(t, filepath.Join(root, "mediator", "pg", "sweep_test.go"), "//go:build integration\n\npackage pg\n\nimport \"testing\"\n\nfunc FuzzTagged(f *testing.F) {}\n")
	// A constraint that is satisfied by default (the host OS) still counts.
	writeFile(t, filepath.Join(root, "mediator", "pg", "os_test.go"), "//go:build "+runtime.GOOS+" || go1.27\n\npackage pg\n\nimport \"testing\"\n\nfunc FuzzHost(f *testing.F) {}\n")
	// A negated tag is satisfied when the tag is absent.
	writeFile(t, filepath.Join(root, "mediator", "pg", "notchaos_test.go"), "//go:build !chaos\n\npackage pg\n\nimport \"testing\"\n\nfunc FuzzNotChaos(f *testing.F) {}\n")
	// Non-test files, testdata, hidden and underscore dirs are ignored.
	writeFile(t, filepath.Join(root, "mediator", "fuzz.go"), "package mediator\n\nimport \"testing\"\n\nfunc FuzzProd(f *testing.F) {}\n")
	writeFile(t, filepath.Join(root, "mediator", "testdata", "x_test.go"), "package x\n\nimport \"testing\"\n\nfunc FuzzData(f *testing.F) {}\n")
	writeFile(t, filepath.Join(root, ".hidden", "x_test.go"), "package x\n\nimport \"testing\"\n\nfunc FuzzHidden(f *testing.F) {}\n")
	writeFile(t, filepath.Join(root, "_old", "x_test.go"), "package x\n\nimport \"testing\"\n\nfunc FuzzOld(f *testing.F) {}\n")
	writeFile(t, filepath.Join(root, "vendor", "x_test.go"), "package x\n\nimport \"testing\"\n\nfunc FuzzVendor(f *testing.F) {}\n")
	// A target at the module root uses the "." pattern.
	writeFile(t, filepath.Join(root, "root_test.go"), "package m\n\nimport \"testing\"\n\nfunc FuzzRoot(f *testing.F) {}\n")

	got, err := discoverFuzzTargets(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []fuzzTarget{
		{".", "FuzzRoot"},
		{"./mediator/pg", "FuzzHost"},
		{"./mediator/pg", "FuzzNotChaos"},
		{"./mediator/validate", "Fuzz"},
		{"./mediator/validate", "FuzzAlso"},
		{"./mediator/validate", "FuzzTagGrammar"},
	}
	if len(got) != len(want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("target %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestDiscoverFuzzTargetsErrors(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "bad_test.go"), "package x\n\nfunc (")
	if _, err := discoverFuzzTargets(root); err == nil {
		t.Error("expected a parse error")
	}
	if _, err := discoverFuzzTargets(filepath.Join(root, "missing")); err == nil {
		t.Error("expected an error for a missing root")
	}
}

func TestBuildsByDefault(t *testing.T) {
	cases := []struct {
		src  string
		want bool
	}{
		{"package x\n", true},
		{"// Copyright\n\n//go:build integration\n\npackage x\n", false},
		{"//go:build !integration\npackage x\n", true},
		{"//go:build " + runtime.GOOS + "\npackage x\n", true},
		{"//go:build " + runtime.GOARCH + " && gc\npackage x\n", true},
		{"//go:build cgo\npackage x\n", false},
		{"//go:build go1.20\npackage x\n", true},
		{"//go:build (integration || chaos) && !windows\npackage x\n", false},
		{"//go:build &&&\npackage x\n", false},
		{"/* block */\n//go:build integration\npackage x\n", true},
		{"package x\n//go:build integration\n", true},
		{"", true},
	}
	for _, tc := range cases {
		if got := buildsByDefault([]byte(tc.src)); got != tc.want {
			t.Errorf("buildsByDefault(%q) = %v, want %v", tc.src, got, tc.want)
		}
	}
}

func TestDefaultTagUnix(t *testing.T) {
	want := runtime.GOOS != "windows" && runtime.GOOS != "plan9" && runtime.GOOS != "js" && runtime.GOOS != "wasip1"
	if got := defaultTag("unix"); got != want {
		t.Errorf("defaultTag(unix) = %v on %s, want %v", got, runtime.GOOS, want)
	}
	if defaultTag("faultinject") {
		t.Error("faultinject must not be set by default")
	}
}
