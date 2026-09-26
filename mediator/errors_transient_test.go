package mediator_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type selfClassified struct{ t bool }

func (s selfClassified) Error() string   { return "self" }
func (s selfClassified) Transient() bool { return s.t }

func TestIsTransient_Transienter(t *testing.T) {
	if !mediator.IsTransient(fmt.Errorf("wrap: %w", selfClassified{t: true})) {
		t.Fatal("Transienter true must be transient")
	}
	if mediator.IsTransient(selfClassified{t: false}) {
		t.Fatal("Transienter false must not be transient by itself")
	}
	if !mediator.IsTransient(errors.Join(selfClassified{false}, mediator.E(mediator.CodeUnavailable, "x"))) {
		t.Fatal("other classifiers still apply")
	}
}
