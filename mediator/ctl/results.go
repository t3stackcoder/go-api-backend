package ctl

import (
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// Result is the typed outcome of a command. Every result marshals to JSON
// with json/v2 (deterministic, camelCase names) and renders a human table
// through WriteTable.
type Result interface {
	// WriteTable writes the human-readable rendering to w.
	WriteTable(w io.Writer) error
}

// Render writes r to w as an indented JSON document when asJSON is set,
// otherwise as its human table.
func Render(w io.Writer, r Result, asJSON bool) error {
	if asJSON {
		return writeJSON(w, r)
	}
	return r.WriteTable(w)
}

// writeJSON marshals v deterministically with two-space indentation and a
// trailing newline.
func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v, json.Deterministic(true), jsontext.WithIndent("  "))
	if err != nil {
		return fmt.Errorf("ctl: encode json: %w", err)
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// table writes header and rows aligned with text/tabwriter; column headers
// are upper-cased.
func table(w io.Writer, header []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for i, h := range header {
		header[i] = strings.ToUpper(h)
	}
	if _, err := fmt.Fprintln(tw, strings.Join(header, "\t")); err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := fmt.Fprintln(tw, strings.Join(r, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// line writes one formatted line to w.
func line(w io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(w, format+"\n", args...)
	return err
}

// formatAge renders a duration rounded to whole seconds.
func formatAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return d.Round(time.Second).String()
}

// formatTime renders a time in UTC RFC 3339, or "-" for the zero time.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

// seconds converts a duration to float seconds for JSON.
func seconds(d time.Duration) float64 { return d.Seconds() }

// MigrationRow is one embedded migration and its state.
type MigrationRow struct {
	Version   int       `json:"version"`
	Name      string    `json:"name"`
	Applied   bool      `json:"applied"`
	AppliedAt time.Time `json:"appliedAt,omitzero"`
}

func (r MigrationRow) file() string { return fmt.Sprintf("%04d_%s", r.Version, r.Name) }

// MigrationStatusResult is the outcome of migrate status.
type MigrationStatusResult struct {
	// Current is the highest applied version, 0 on an empty database.
	Current    int            `json:"current"`
	Migrations []MigrationRow `json:"migrations"`
}

// WriteTable implements Result.
func (r *MigrationStatusResult) WriteTable(w io.Writer) error {
	rows := make([][]string, 0, len(r.Migrations))
	for _, m := range r.Migrations {
		rows = append(rows, []string{strconv.Itoa(m.Version), m.Name, strconv.FormatBool(m.Applied), formatTime(m.AppliedAt)})
	}
	if err := table(w, []string{"version", "name", "applied", "applied at"}, rows); err != nil {
		return err
	}
	return line(w, "current version: %d", r.Current)
}

// MigrateResult is the outcome of migrate up and migrate down.
type MigrateResult struct {
	// Direction is "up" or "down".
	Direction string `json:"direction"`
	// Before and Current are the highest applied versions before and after.
	Before  int `json:"before"`
	Current int `json:"current"`
	// Changed lists the migrations applied (up) or reverted (down), in the
	// order they were processed.
	Changed []MigrationRow `json:"changed"`
}

// WriteTable implements Result.
func (r *MigrateResult) WriteTable(w io.Writer) error {
	verb := "applied"
	if r.Direction == "down" {
		verb = "reverted"
	}
	if len(r.Changed) == 0 {
		return line(w, "nothing to do: current version %d", r.Current)
	}
	for _, m := range r.Changed {
		if err := line(w, "%s %s", verb, m.file()); err != nil {
			return err
		}
	}
	return line(w, "current version: %d (was %d)", r.Current, r.Before)
}

// NameRow is one persisted identifier the registry derives.
type NameRow struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	GoType string `json:"goType"`
	Group  string `json:"group"`
	Topic  string `json:"topic"`
	Pinned bool   `json:"pinned"`
}

// NamesResult is the outcome of names.
type NamesResult struct {
	Names []NameRow `json:"names"`
}

// WriteTable implements Result.
func (r *NamesResult) WriteTable(w io.Writer) error {
	if len(r.Names) == 0 {
		return line(w, "no registrations")
	}
	rows := make([][]string, 0, len(r.Names))
	for _, n := range r.Names {
		rows = append(rows, []string{n.Kind, n.Name, n.GoType, dash(n.Group), dash(n.Topic), strconv.FormatBool(n.Pinned)})
	}
	return table(w, []string{"kind", "name", "go type", "group", "topic", "pinned"}, rows)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// OutboxPartitionRow is the unpublished backlog of one (topic, partition).
type OutboxPartitionRow struct {
	Topic            string  `json:"topic"`
	Partition        int     `json:"partition"`
	Unpublished      int64   `json:"unpublished"`
	OldestAgeSeconds float64 `json:"oldestAgeSeconds"`
}

// OutboxStatsResult is the outcome of outbox stats. Partitions without a
// backlog are omitted.
type OutboxStatsResult struct {
	Partitions []OutboxPartitionRow `json:"partitions"`
}

// WriteTable implements Result.
func (r *OutboxStatsResult) WriteTable(w io.Writer) error {
	if len(r.Partitions) == 0 {
		return line(w, "no unpublished outbox rows")
	}
	rows := make([][]string, 0, len(r.Partitions))
	for _, p := range r.Partitions {
		rows = append(rows, []string{p.Topic, strconv.Itoa(p.Partition), strconv.FormatInt(p.Unpublished, 10),
			formatAge(time.Duration(p.OldestAgeSeconds * float64(time.Second)))})
	}
	return table(w, []string{"topic", "partition", "unpublished", "oldest age"}, rows)
}

// OutboxReplayResult is the outcome of outbox replay.
type OutboxReplayResult struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	FromID    int64  `json:"fromId"`
	Replayed  int    `json:"replayed"`
}

// WriteTable implements Result.
func (r *OutboxReplayResult) WriteTable(w io.Writer) error {
	return line(w, "replayed %d rows of %s partition %d from id %d", r.Replayed, r.Topic, r.Partition, r.FromID)
}

// OutboxReshardResult is the outcome of outbox reshard.
type OutboxReshardResult struct {
	Partitions int   `json:"partitions"`
	Moved      int64 `json:"moved"`
}

// WriteTable implements Result.
func (r *OutboxReshardResult) WriteTable(w io.Writer) error {
	return line(w, "resharded to %d partitions: %d rows moved, relay cursors reset", r.Partitions, r.Moved)
}

// PurgeResult is the outcome of inbox purge and idem purge.
type PurgeResult struct {
	Table string `json:"table"`
	// OlderThan is the retention argument of inbox purge, empty for idem purge.
	OlderThan string `json:"olderThan,omitzero"`
	Deleted   int64  `json:"deleted"`
}

// WriteTable implements Result.
func (r *PurgeResult) WriteTable(w io.Writer) error {
	if r.OlderThan != "" {
		return line(w, "deleted %d rows of %s older than %s", r.Deleted, r.Table, r.OlderThan)
	}
	return line(w, "deleted %d expired rows of %s", r.Deleted, r.Table)
}

// IdemShowResult is the outcome of idem show: one idempotency row.
type IdemShowResult struct {
	Scope string `json:"scope"`
	Key   string `json:"key"`
	// RequestHash is the canonical request hash in hex.
	RequestHash string `json:"requestHash"`
	// Response is the stored JSON response, absent while the row is only
	// reserved.
	Response  any       `json:"response,omitzero"`
	Hits      int       `json:"hits"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	// Expired is true when ExpiresAt has passed at Options.Now.
	Expired bool `json:"expired"`
}

// WriteTable implements Result.
func (r *IdemShowResult) WriteTable(w io.Writer) error {
	response := "(reserved, no response yet)"
	if v, ok := r.Response.(jsontext.Value); ok {
		response = string(v)
	}
	rows := [][]string{
		{"scope", r.Scope},
		{"key", r.Key},
		{"request hash", r.RequestHash},
		{"hits", strconv.Itoa(r.Hits)},
		{"created at", formatTime(r.CreatedAt)},
		{"expires at", formatTime(r.ExpiresAt)},
		{"expired", strconv.FormatBool(r.Expired)},
		{"response", response},
	}
	return table(w, []string{"field", "value"}, rows)
}

// ConsumerLagRow is the pending summary of one (group, topic, partition)
// together with the unpublished outbox backlog of the partition.
type ConsumerLagRow struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	// Pending is the number of delivered, unacknowledged entries.
	Pending int64 `json:"pending"`
	// OldestAgeSeconds is the age of the oldest pending entry.
	OldestAgeSeconds float64 `json:"oldestAgeSeconds"`
	// Consumers is the pending count per consumer name.
	Consumers map[string]int64 `json:"consumers"`
	// Unpublished is the outbox backlog not yet relayed to the stream.
	Unpublished int64 `json:"unpublished"`
	// Missing is set when the stream or the group does not exist.
	Missing bool `json:"missing"`
}

// ConsumerLagResult is the outcome of consumer lag.
type ConsumerLagResult struct {
	Rows []ConsumerLagRow `json:"rows"`
}

// WriteTable implements Result.
func (r *ConsumerLagResult) WriteTable(w io.Writer) error {
	if len(r.Rows) == 0 {
		return line(w, "no groups or topics selected")
	}
	rows := make([][]string, 0, len(r.Rows))
	for _, l := range r.Rows {
		rows = append(rows, []string{l.Group, l.Topic, strconv.Itoa(l.Partition), strconv.FormatInt(l.Pending, 10),
			formatAge(time.Duration(l.OldestAgeSeconds * float64(time.Second))), strconv.FormatInt(l.Unpublished, 10),
			formatConsumers(l.Consumers), strconv.FormatBool(l.Missing)})
	}
	return table(w, []string{"group", "topic", "partition", "pending", "oldest age", "unpublished", "consumers", "missing"}, rows)
}

// formatConsumers renders "name=count" pairs sorted by name, or "-".
func formatConsumers(c map[string]int64) string {
	if len(c) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(c))
	for name, n := range c {
		parts = append(parts, name+"="+strconv.FormatInt(n, 10))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// DLQRow is one dead letter.
type DLQRow struct {
	// ID is the entry ID in the dead-letter stream, the argument of dlq
	// requeue and dlq drop.
	ID string `json:"id"`
	// Stream and StreamID locate the original entry.
	Stream   string    `json:"stream"`
	StreamID string    `json:"streamId"`
	Attempts int       `json:"attempts"`
	Error    string    `json:"error"`
	Node     string    `json:"node"`
	FailedAt time.Time `json:"failedAt,omitzero"`
	// Event fields decoded from the original entry.
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Seq       int64  `json:"seq"`
	Partition int    `json:"partition"`
	OutboxID  int64  `json:"outboxId"`
	// DecodeError is set when the original fields could not be decoded.
	DecodeError string `json:"decodeError,omitzero"`
}

// DLQListResult is the outcome of dlq list.
type DLQListResult struct {
	Group   string   `json:"group"`
	Entries []DLQRow `json:"entries"`
}

// WriteTable implements Result.
func (r *DLQListResult) WriteTable(w io.Writer) error {
	if len(r.Entries) == 0 {
		return line(w, "no dead letters in group %s", r.Group)
	}
	rows := make([][]string, 0, len(r.Entries))
	for _, e := range r.Entries {
		typ := e.EventType
		if e.DecodeError != "" {
			typ = "(undecodable: " + e.DecodeError + ")"
		}
		rows = append(rows, []string{e.ID, typ, dash(e.Key), strconv.FormatInt(e.Seq, 10), strconv.Itoa(e.Attempts),
			formatTime(e.FailedAt), dash(e.Node), e.Error})
	}
	return table(w, []string{"id", "event", "key", "seq", "attempts", "failed at", "node", "error"}, rows)
}

// DLQActionResult is the outcome of dlq requeue and dlq drop.
type DLQActionResult struct {
	Group string `json:"group"`
	ID    string `json:"id"`
	// Action is "requeued" or "dropped".
	Action string `json:"action"`
}

// WriteTable implements Result.
func (r *DLQActionResult) WriteTable(w io.Writer) error {
	return line(w, "%s dead letter %s of group %s", r.Action, r.ID, r.Group)
}

// PartitionSkipResult is the outcome of dlq skip.
type PartitionSkipResult struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	ID        string `json:"id"`
}

// WriteTable implements Result.
func (r *PartitionSkipResult) WriteTable(w io.Writer) error {
	return line(w, "acknowledged %s on %s partition %d for group %s; a halted consumer resumes at its next lease renewal",
		r.ID, r.Topic, r.Partition, r.Group)
}

// LeaseRow is one partition lease.
type LeaseRow struct {
	Group      string  `json:"group"`
	Topic      string  `json:"topic"`
	Partition  int     `json:"partition"`
	Node       string  `json:"node"`
	Epoch      int64   `json:"epoch"`
	TTLSeconds float64 `json:"ttlSeconds"`
}

// LeaseListResult is the outcome of lease list.
type LeaseListResult struct {
	Group  string     `json:"group"`
	Leases []LeaseRow `json:"leases"`
}

// WriteTable implements Result.
func (r *LeaseListResult) WriteTable(w io.Writer) error {
	if len(r.Leases) == 0 {
		return line(w, "no leases held in group %s", r.Group)
	}
	rows := make([][]string, 0, len(r.Leases))
	for _, l := range r.Leases {
		rows = append(rows, []string{l.Topic, strconv.Itoa(l.Partition), l.Node, strconv.FormatInt(l.Epoch, 10),
			formatAge(time.Duration(l.TTLSeconds * float64(time.Second)))})
	}
	return table(w, []string{"topic", "partition", "node", "epoch", "ttl"}, rows)
}

// LeaseReleaseResult is the outcome of lease release.
type LeaseReleaseResult struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Released  bool   `json:"released"`
}

// WriteTable implements Result.
func (r *LeaseReleaseResult) WriteTable(w io.Writer) error {
	return line(w, "released lease of %s partition %d in group %s; the next owner drains pending entries first",
		r.Topic, r.Partition, r.Group)
}

// OpenAPIExportResult is the outcome of openapi export.
type OpenAPIExportResult struct {
	Out     string `json:"out"`
	Title   string `json:"title"`
	Version string `json:"version"`
	Prefix  string `json:"prefix,omitzero"`
}

// WriteTable implements Result.
func (r *OpenAPIExportResult) WriteTable(w io.Writer) error {
	return line(w, "wrote %s (%s %s)", r.Out, r.Title, r.Version)
}

// hexBytes renders b in hex, or "-" when empty.
func hexBytes(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	return hex.EncodeToString(b)
}
