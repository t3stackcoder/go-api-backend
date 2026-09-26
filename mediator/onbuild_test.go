package mediator_test

import (
	"errors"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// TestOnBuild_HooksMayEnumerateRegistry guards against Build holding the
// registry lock while hooks call the public accessors (a deadlock found by
// the httpapi package).
func TestOnBuild_HooksMayEnumerateRegistry(t *testing.T) {
	m := mediator.New()
	mediator.MustHandle(m, mediator.HandlerFunc[ping, mediator.Void](func(ctx0 ctxT, p ping) (mediator.Void, error) { return mediator.Void{}, nil }))
	var seen int
	_ = m.OnBuild(func(m *mediator.Mediator) error {
		seen = len(m.Requests())
		if _, ok := m.InfoOf(reflectTypeOf(ping{})); !ok {
			return errors.New("InfoOf failed inside hook")
		}
		_ = m.ConsumerRegistrations()
		_ = m.Events()
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- m.Build() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-timeoutAfter():
		t.Fatal("Build deadlocked while an OnBuild hook enumerated the registry")
	}
	if seen != 1 {
		t.Fatalf("hook saw %d requests", seen)
	}
}
