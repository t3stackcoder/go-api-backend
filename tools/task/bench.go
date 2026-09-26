package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
)

// benchDelta is one row of a benchstat A/B comparison.
type benchDelta struct {
	// Name is the benchmark as benchstat prints it, e.g. Send_DefaultChain-8.
	Name string
	// Metric is the unit of the table the row came from: sec/op, B/op, allocs/op.
	Metric string
	// Percent is the signed change of the median; positive means slower or bigger.
	// It is zero when the change is not significant.
	Percent float64
	// Significant is false when benchstat printed "~" (p at or above alpha).
	Significant bool
	// P is the statistics column, e.g. "p=0.002 n=6".
	P string
}

// parseBenchstatCSV parses the output of `benchstat -format csv old new`.
//
// The CSV form is used because it is stable and needs no column-width
// guessing. It consists of a preamble (goos, goarch, pkg, cpu lines), then
// one table per metric. Each table has a file-name row, a header row, data
// rows, and a geomean row:
//
//	,old.txt,,new.txt,,,
//	,sec/op,CI,sec/op,CI,vs base,P
//	Send_DefaultChain-8,1.001e-06,1%,1.301e-06,1%,+29.97%,p=0.002 n=6
//	Publish_InProcess-8,2.001e-06,1%,2.001e-06,1%,~,p=1.000 n=6
//	geomean,1.415e-06,,1.613e-06,,+14.00%,
//
// The header row is recognized by an empty first field and "vs base" in the
// sixth; its second field names the metric. Data rows carry the benchmark
// name in the first field, the signed percentage or "~" in the sixth, and
// the p-value in the seventh. The geomean row is ignored. Rows with fewer
// than seven fields (a benchmark present in only one file) are skipped.
func parseBenchstatCSV(s string) ([]benchDelta, error) {
	r := csv.NewReader(strings.NewReader(s))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("benchstat csv: %w", err)
	}
	var deltas []benchDelta
	metric := ""
	for _, rec := range records {
		if len(rec) >= 6 && rec[0] == "" && rec[5] == "vs base" {
			metric = rec[1]
			continue
		}
		if metric == "" || len(rec) < 7 || rec[0] == "" || rec[0] == "geomean" {
			continue
		}
		d := benchDelta{Name: rec[0], Metric: metric, P: rec[6]}
		if v := strings.TrimSpace(rec[5]); v != "~" && v != "" {
			pct, err := strconv.ParseFloat(strings.TrimSuffix(v, "%"), 64)
			if err != nil {
				return nil, fmt.Errorf("benchstat csv: row %q: bad delta %q", rec[0], v)
			}
			d.Percent = pct
			d.Significant = true
		}
		deltas = append(deltas, d)
	}
	return deltas, nil
}

// regressions returns the significant rows of metric whose median grew by
// more than threshold percent. Rows benchstat marks "~" are noise by its own
// test and never count.
func regressions(deltas []benchDelta, metric string, threshold float64) []benchDelta {
	var out []benchDelta
	for _, d := range deltas {
		if d.Metric == metric && d.Significant && d.Percent > threshold {
			out = append(out, d)
		}
	}
	return out
}

// countMetric counts the rows of one metric.
func countMetric(deltas []benchDelta, metric string) int {
	n := 0
	for _, d := range deltas {
		if d.Metric == metric {
			n++
		}
	}
	return n
}
