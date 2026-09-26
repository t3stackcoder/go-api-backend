//go:build !faultinject

package main

import (
	"errors"
	"io"
)

// faultInjectionEnabled reports whether /chaos/fault can arm schedules. This
// binary was built without the faultinject tag, so it cannot.
const faultInjectionEnabled = false

// faultSchedule is the JSON form of one schedule; unused in this build.
type faultSchedule struct {
	Point string `json:"point"`
	Kind  string `json:"kind"`
	Hit   int    `json:"hit,omitzero"`
	Delay string `json:"delay,omitzero"`
}

var errNoFaultInjection = errors.New("chaosnode: built without the faultinject tag")

// armFaults always fails in this build.
func armFaults(io.Reader) ([]faultSchedule, error) { return nil, errNoFaultInjection }

// disarmFaults is a no-op in this build.
func disarmFaults() {}

// faultStatus reports that fault injection is unavailable.
func faultStatus() map[string]any { return map[string]any{"enabled": false} }
