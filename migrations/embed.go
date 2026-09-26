// Package migrations embeds the SQL migrations of the mediator framework.
// Files are named NNNN_name.sql and contain the forward DDL followed by an
// optional "-- down" section that reverts it (spec 6.7). pg.Migrate applies
// them; pg.MigrateDown (tests only) reverts them.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
