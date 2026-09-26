//go:build !faultinject

// Package testkit holds the test infrastructure shared by every tier: fault
// points, the injectable clock, and the recorded-history types.
//
// Without the faultinject build tag every fault point compiles to a nil
// return that the compiler inlines away, so production binaries carry no
// cost. With the tag, testkit.Arm schedules faults by point name.
package testkit

import "context"

// FaultInjectionEnabled reports whether this binary was built with the
// faultinject tag.
const FaultInjectionEnabled = false

// Fault is the I/O call-site hook. Every I/O operation in pg, redisx, and the
// behaviors calls it with a stable point name before (and, for the ambiguous
// and crash kinds, after) performing the operation. In this build it is a no-op.
func Fault(ctx context.Context, point string) error { return nil }

// FaultAfter is called after an operation succeeded so the ambiguous and crash
// fault kinds can act. In this build it is a no-op.
func FaultAfter(ctx context.Context, point string) error { return nil }
