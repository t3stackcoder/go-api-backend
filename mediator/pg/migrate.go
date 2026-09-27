package pg

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/migrations"
)

// MigrationInfo is one row of MigrationStatus.
type MigrationInfo struct {
	Version   int
	Name      string
	Applied   bool
	AppliedAt time.Time
}

// migration is one parsed NNNN_name.sql file.
type migration struct {
	version int
	name    string
	up      string
	down    string
}

func (m migration) String() string { return fmt.Sprintf("%04d_%s", m.version, m.name) }

var (
	migrationFile = regexp.MustCompile(`^(\d{4})_([A-Za-z0-9_]+)\.sql$`)
	downMarker    = regexp.MustCompile(`(?mi)^--\s*down\s*$`)
)

// migrationsFS is the migration source of Migrate, MigrateDown, and
// MigrationStatus. Tests swap it for a malformed set.
var migrationsFS fs.FS = migrations.FS

// loadMigrations parses every NNNN_name.sql of fsys, splitting the optional
// "-- down" section, sorted by version.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("pg: read migrations: %w", err)
	}
	var out []migration
	seen := map[int]string{}
	for _, e := range entries {
		m := migrationFile.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			continue
		}
		version, _ := strconv.Atoi(m[1])
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("pg: migrations %s and %s share version %d", prev, e.Name(), version)
		}
		seen[version] = e.Name()
		src, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("pg: read migration %s: %w", e.Name(), err)
		}
		up, down := splitMigration(string(src))
		out = append(out, migration{version: version, name: m[2], up: up, down: down})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// splitMigration separates the forward SQL from the "-- down" section.
func splitMigration(src string) (up, down string) {
	loc := downMarker.FindStringIndex(src)
	if loc == nil {
		return strings.TrimSpace(src), ""
	}
	return strings.TrimSpace(src[:loc[0]]), strings.TrimSpace(src[loc[1]:])
}

const (
	sqlMigrateLock   = `SELECT pg_advisory_lock(hashtext('mediator_migrate'))`
	sqlMigrateUnlock = `SELECT pg_advisory_unlock(hashtext('mediator_migrate'))`
	sqlVersionTable  = `SELECT to_regclass('mediator_schema_version')::text`
	sqlVersions      = `SELECT version, applied_at FROM mediator_schema_version ORDER BY version`
	sqlRecordVersion = `INSERT INTO mediator_schema_version (version) VALUES ($1)`
	sqlForgetVersion = `DELETE FROM mediator_schema_version WHERE version = $1`
)

// withMigrateLock runs f on one connection holding the migration advisory
// lock (6.7), so concurrent Migrate calls serialize.
func withMigrateLock(ctx context.Context, pool *pgxpool.Pool, f func(ctx context.Context, conn *pgxpool.Conn) error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pg: migrate: acquire connection: %w", err)
	}
	if _, err := conn.Exec(ctx, sqlMigrateLock); err != nil {
		conn.Release()
		return fmt.Errorf("pg: migrate: lock: %w", err)
	}
	ferr := f(ctx, conn)
	if _, err := conn.Exec(context.WithoutCancel(ctx), sqlMigrateUnlock); err != nil {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Hijack().Close(cctx)
		return errors.Join(ferr, fmt.Errorf("pg: migrate: unlock: %w", err))
	}
	conn.Release()
	return ferr
}

// appliedVersions reads mediator_schema_version, or an empty map when the
// table does not exist yet (fresh database).
func appliedVersions(ctx context.Context, conn *pgxpool.Conn) (map[int]time.Time, error) {
	var table *string
	if err := conn.QueryRow(ctx, sqlVersionTable).Scan(&table); err != nil {
		return nil, fmt.Errorf("pg: migrate: version table: %w", err)
	}
	out := map[int]time.Time{}
	if table == nil {
		return out, nil
	}
	rows, err := conn.Query(ctx, sqlVersions)
	if err != nil {
		return nil, fmt.Errorf("pg: migrate: versions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		var at time.Time
		if err := rows.Scan(&v, &at); err != nil {
			return nil, fmt.Errorf("pg: migrate: versions: %w", err)
		}
		out[v] = at
	}
	return out, rows.Err()
}

// inTx runs f in one transaction on conn.
func inTx(ctx context.Context, conn *pgxpool.Conn, f func(tx pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return tx.Commit(ctx)
}

// Migrate takes the migration advisory lock, reads mediator_schema_version,
// and applies every embedded migration with a higher version than the
// current one, each in its own transaction, recording it (6.7).
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	ms, err := loadMigrations(migrationsFS)
	if err != nil {
		return err
	}
	return withMigrateLock(ctx, pool, func(ctx context.Context, conn *pgxpool.Conn) error {
		applied, err := appliedVersions(ctx, conn)
		if err != nil {
			return err
		}
		current := 0
		for v := range applied {
			current = max(current, v)
		}
		for _, m := range ms {
			if m.version <= current {
				continue
			}
			if err := testkit.Fault(ctx, "pg.migrate.apply"); err != nil {
				return err
			}
			err := inTx(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, m.up); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, sqlRecordVersion, m.version)
				return err
			})
			if err != nil {
				return fmt.Errorf("pg: migration %s: %w", m, err)
			}
		}
		return nil
	})
}

// MigrateDown reverts applied migrations with a version above toVersion, in
// descending order, using their "-- down" sections. It exists for tests of
// 6.7; migrations are append-only once merged.
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, toVersion int) error {
	ms, err := loadMigrations(migrationsFS)
	if err != nil {
		return err
	}
	return withMigrateLock(ctx, pool, func(ctx context.Context, conn *pgxpool.Conn) error {
		applied, err := appliedVersions(ctx, conn)
		if err != nil {
			return err
		}
		for i := len(ms) - 1; i >= 0; i-- {
			m := ms[i]
			if m.version <= toVersion {
				continue
			}
			if _, ok := applied[m.version]; !ok {
				continue
			}
			if m.down == "" {
				return fmt.Errorf("pg: migration %s has no down section", m)
			}
			if err := testkit.Fault(ctx, "pg.migrate.apply"); err != nil {
				return err
			}
			// Forget the version first: the down section of the first
			// migration drops the version table itself.
			err := inTx(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, sqlForgetVersion, m.version); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, m.down)
				return err
			})
			if err != nil {
				return fmt.Errorf("pg: revert migration %s: %w", m, err)
			}
		}
		return nil
	})
}

// MigrationStatus lists every embedded migration with whether and when it
// was applied (mediatorctl migrate status).
func MigrationStatus(ctx context.Context, pool *pgxpool.Pool) ([]MigrationInfo, error) {
	ms, err := loadMigrations(migrationsFS)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg: migrate: acquire connection: %w", err)
	}
	defer conn.Release()
	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return nil, err
	}
	out := make([]MigrationInfo, 0, len(ms))
	for _, m := range ms {
		at, ok := applied[m.version]
		out = append(out, MigrationInfo{Version: m.version, Name: m.name, Applied: ok, AppliedAt: at})
	}
	return out, nil
}
