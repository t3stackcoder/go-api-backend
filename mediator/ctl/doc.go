// Package ctl implements every command of mediatorctl (spec 9.3) as a Go
// function with a typed result, and Main, the argument parser and renderer
// the cmd/mediatorctl binary is a thin shell around.
//
// # Commands
//
//	migrate up | status | down --to N
//	names
//	outbox stats | replay --topic T --partition N --from-id X | reshard --partitions N --yes
//	inbox purge --older-than 7d
//	idem purge | show --scope S --key K
//	consumer lag [--group G] [--topic T]
//	dlq list --group G | requeue --group G --id ID | drop --group G --id ID
//	dlq skip --group G --topic T --partition N --id ID
//	lease list --group G | release --group G --topic T --partition N
//	openapi export --out api/openapi.json [--title T] [--version V] [--prefix P]
//
// Every command accepts --json for deterministic machine output and -h for
// its flags. Exit codes: 0 success, 1 the command failed, 2 usage error,
// 130 interrupted (context canceled).
//
// # Configuration
//
// Nothing in the framework packages reads the environment (spec 9.4); the
// CLI is the application edge, so this package is where the environment is
// read. Zero fields of Options take their value from PG_URL, REDIS_ADDR,
// REDIS_USERNAME, REDIS_PASSWORD, REDIS_DB, and REDIS_PREFIX. Connections are
// opened lazily: Postgres only for the migrate, outbox, inbox, and idem
// commands, Redis only for the dlq and lease commands, both for outbox
// replay and consumer lag, and neither for names and openapi export, which
// need only the registry.
//
// # Testing
//
// The Postgres and Redis interfaces are the seams: the commands are written
// against them, NewPostgres and NewRedis adapt a real pool and client, and
// tests substitute fakes so the rendering and argument parsing of every
// command run without containers.
package ctl
