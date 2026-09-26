//go:build faultinject

package main

import (
	"encoding/json/v2"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// faultInjectionEnabled reports whether /chaos/fault can arm schedules.
const faultInjectionEnabled = true

// faultSchedule is the JSON form of one testkit.Schedule accepted by
// PUT /chaos/fault: {"point":"pg.outbox.insert","kind":"error","hit":1,"delay":"100ms"}.
type faultSchedule struct {
	Point string `json:"point"`
	Kind  string `json:"kind"`
	Hit   int    `json:"hit,omitzero"`
	Delay string `json:"delay,omitzero"`
}

// armFaults decodes a JSON array (or a single object) of schedules and arms
// them, replacing any earlier schedule. It returns the armed schedules.
func armFaults(body io.Reader) ([]faultSchedule, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	var list []faultSchedule
	if len(raw) > 0 && raw[0] == '{' {
		var one faultSchedule
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, fmt.Errorf("decode schedule: %w", err)
		}
		list = []faultSchedule{one}
	} else if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("decode schedules: %w", err)
	}
	kinds := map[string]bool{}
	for _, k := range testkit.AllFaultKinds {
		kinds[string(k)] = true
	}
	schedules := make([]testkit.Schedule, 0, len(list))
	for i, s := range list {
		if s.Point == "" {
			return nil, fmt.Errorf("schedule %d: point is required", i)
		}
		if !kinds[s.Kind] {
			return nil, fmt.Errorf("schedule %d: unknown fault kind %q", i, s.Kind)
		}
		sc := testkit.Schedule{Point: s.Point, Kind: testkit.FaultKind(s.Kind), Hit: s.Hit}
		if s.Delay != "" {
			d, err := time.ParseDuration(s.Delay)
			if err != nil {
				return nil, fmt.Errorf("schedule %d: delay: %w", i, err)
			}
			sc.Delay = d
		}
		schedules = append(schedules, sc)
	}
	testkit.Arm(schedules...)
	return list, nil
}

// disarmFaults removes every armed schedule.
func disarmFaults() { testkit.Disarm() }

// faultStatus reports the hit counts since the last Arm and every point
// observed since process start.
func faultStatus() map[string]any {
	observed := testkit.Observed()
	sort.Strings(observed)
	return map[string]any{"enabled": true, "hits": testkit.Hits(), "observed": observed}
}
