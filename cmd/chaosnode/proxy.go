package main

import (
	"context"
	"errors"
	"sort"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// WorkloadCommands lists the workload command names the proxy mediator can
// Declare for remote dispatch.
var WorkloadCommands = []string{
	workload.NameSetValue, workload.NameTransfer, workload.NameAppend, workload.NameBump,
	workload.NameAtomicScenario, workload.NamePanic, workload.NameSlow, workload.NameTouch,
}

// forward registers a handler on mp that sends the request through m, so
// the full local chain of m runs (unit of work, idempotency, cache).
func forward[Q mediator.Request[R], R any](mp, m *mediator.Mediator) error {
	return mediator.HandleFunc(mp, func(ctx context.Context, q Q) (R, error) {
		return mediator.Send(ctx, m, q)
	})
}

// proxyCommand registers a command on mp: Declared (remote dispatch) unless
// it is kept local, in which case it forwards to m.
func proxyCommand[Q mediator.Request[R], R any](mp, m *mediator.Mediator, name string, local map[string]bool) (bool, error) {
	if local[name] {
		return false, forward[Q, R](mp, m)
	}
	return true, mediator.Declare[Q, R](mp)
}

// registerProxy fills the proxy mediator of remote dispatch mode: every
// workload query forwards to the full mediator m (they need its unit of
// work and cache), and every workload command is Declared so that Send
// dispatches it to a serving node (spec 7.6), except the commands named in
// local, which forward to m. It returns the names that were Declared.
func registerProxy(mp, m *mediator.Mediator, local map[string]bool) ([]string, error) {
	var errs []error
	var remote []string
	add := func(name string, declared bool, err error) {
		errs = append(errs, err)
		if declared && err == nil {
			remote = append(remote, name)
		}
	}
	errs = append(errs,
		forward[workload.GetValue, workload.ValueView](mp, m),
		forward[workload.GetValueCached, workload.ValueView](mp, m),
		forward[workload.ReadAll, workload.BankView](mp, m),
		forward[workload.ReadList, []int64](mp, m),
	)
	d, err := proxyCommand[workload.SetValue, mediator.Void](mp, m, workload.NameSetValue, local)
	add(workload.NameSetValue, d, err)
	d, err = proxyCommand[workload.Transfer, mediator.Void](mp, m, workload.NameTransfer, local)
	add(workload.NameTransfer, d, err)
	d, err = proxyCommand[workload.Append, workload.AppendResult](mp, m, workload.NameAppend, local)
	add(workload.NameAppend, d, err)
	d, err = proxyCommand[workload.Bump, workload.BumpResult](mp, m, workload.NameBump, local)
	add(workload.NameBump, d, err)
	d, err = proxyCommand[workload.AtomicScenario, mediator.Void](mp, m, workload.NameAtomicScenario, local)
	add(workload.NameAtomicScenario, d, err)
	d, err = proxyCommand[workload.Panic, mediator.Void](mp, m, workload.NamePanic, local)
	add(workload.NamePanic, d, err)
	d, err = proxyCommand[workload.Slow, mediator.Void](mp, m, workload.NameSlow, local)
	add(workload.NameSlow, d, err)
	d, err = proxyCommand[workload.Touch, mediator.Void](mp, m, workload.NameTouch, local)
	add(workload.NameTouch, d, err)
	sort.Strings(remote)
	return remote, errors.Join(errs...)
}
