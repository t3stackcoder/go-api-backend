package behavior_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

var errWholeRequest = errors.New("whole request rejected")

// crossFieldCmd fails its Validate method when From > To.
type crossFieldCmd struct {
	mediator.Command[mediator.Void]
	From int `json:"from" validate:"min=0"`
	To   int `json:"to"`
}

func (c crossFieldCmd) Validate(context.Context) error {
	if c.From > c.To {
		return errWholeRequest
	}
	return nil
}

type badTagCmd struct {
	mediator.Command[mediator.Void]
	Name string `json:"name" validate:"nosuchrule"`
}

func TestValidation_Check(t *testing.T) {
	h := newHarness(t, withPreBuild(func(m *mediator.Mediator) error {
		return mediator.HandleFunc(m, func(context.Context, crossFieldCmd) (mediator.Void, error) { return mediator.Void{}, nil })
	}))
	ctx := context.Background()
	_, err := mediator.Send(ctx, h.m, plainCmd{})
	var ve *mediator.ValidationError
	if !errors.As(err, &ve) || len(ve.Fields) != 1 || ve.Fields[0].Path != "/id" || ve.Fields[0].Rule != "required" {
		t.Fatalf("want required failure on /id, got %v", err)
	}
	if h.store.Begun() != 0 {
		t.Fatal("validation must fail before the unit of work opens")
	}
	if _, err := mediator.Send(ctx, h.m, crossFieldCmd{From: 2, To: 1}); !errors.Is(err, errWholeRequest) {
		t.Fatalf("Validate error must be returned unchanged: %v", err)
	}
	if _, err := mediator.Send(ctx, h.m, crossFieldCmd{From: -1, To: 1}); mediator.CodeOf(err) != mediator.CodeValidation {
		t.Fatalf("tag rules run first: %v", err)
	}
	if _, err := mediator.Send(ctx, h.m, crossFieldCmd{From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
}

func TestValidation_PrepareRejectsUnknownRule(t *testing.T) {
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, badTagCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, mediator.OnFunc(m, func(context.Context, thingEvent) error { return nil }))
	must(t, behavior.UseStandard(m, behavior.Config{Validator: validate.New()}))
	err := m.Build()
	if err == nil || !strings.Contains(err.Error(), "badTagCmd") || !strings.Contains(err.Error(), "nosuchrule") {
		t.Fatalf("Build = %v", err)
	}
	if b := behavior.NewValidation(behavior.Config{}); b.Name() != behavior.Validation {
		t.Fatal(b.Name())
	}
}
