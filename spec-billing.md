# Billing: a usage-based billing service on the mediator framework

| | |
|---|---|
| Status | Draft v0.1, for review |
| Date | 2026-09-27 |
| Repository | `go-api-backend` |
| Module path | `github.com/t3stackcoder/go-api-backend` |
| Depends on | `spec.md` (the framework contract, v0.2 as implemented; see `docs/design-notes.md` for the deviations) |
| Toolchain | Go 1.27, Postgres 18, Redis 8, Docker Compose v2, Polar API (`https://api.polar.sh`, sandbox `https://sandbox-api.polar.sh`) |

This document is the complete specification of the billing system: the domain model, the schema, every request and event, the Polar integration protocol in both directions, the HTTP surface, the operational surface, the precise guarantees the system makes, and the test program that verifies every one of those guarantees. The system is an application module on the mediator framework and is held to the same standard `spec.md` section 11 holds the framework to: every guarantee in section 13 names the test that would catch its violation, and every tier from static analysis to the chaos matrix runs against it.

Section numbers of the form "spec 6.3" refer to `spec.md`; unqualified section numbers refer to this document. Where this document and `spec.md` disagree about the framework, `spec.md` wins; where they disagree about billing, this document wins until it is amended.

---

## 0. Decisions already confirmed

These were agreed before writing and are treated as fixed:

1. **Go, on the mediator framework, as an application module.** The billing system registers requests, events, handlers, and consumers on a mediator exactly as `examples/orders` does, runs under `mediator.Runtime`, and is served by `httpapi`. It changes nothing in `mediator/` except the additive generalizations listed in 3.5, each recorded in `docs/design-notes.md` section 6 before it is made.
2. **The same test program.** Tiers 0 to 6 of spec 11 apply. The billing system contributes its own unit tables, property tests, fuzz targets, fault sweep scenarios, invariants, integration scenarios, chaos workloads and nemeses, benchmarks, and mutation and coverage gates. A guarantee without a test does not go in section 13.
3. **Backend first, no UI.** The interface is HTTP with a generated OpenAPI 3.1 document and a TypeScript client built from it in CI. Nothing in this document depends on a front end existing.
4. **Identity is given.** A user ID exists (a Better Auth `user.id`). When the Better Auth organizations plugin is in use, the active organization ID is also given. The framework's `authz.Principal.Subject` carries the user ID and `Principal.Tenant` the organization ID (4.1).
5. **Polar is the payment provider and the merchant of record.** Polar owns money: products and prices, checkout, subscriptions, invoices (orders), payments, refunds, taxes, and the customer portal. This system owns usage: the ledger of what happened, the entitlements that decide what an account may do, the statements that explain what a period will cost, and the reconciliation that proves Polar and the ledger agree.
6. **Better Auth's Polar convention is honored, not depended on.** The Polar customer of an account carries `external_id` equal to the account's external ID, which is the Better Auth user ID for a user account and the organization ID for an organization account. That is the convention of `@polar-sh/better-auth` (`createCustomerOnSignUp` sets `externalId` to `user.id`), so the two can coexist, but this backend creates and looks up Polar customers itself and never needs the plugin.
7. **Usage is recorded here first and ingested into Polar second.** Polar's meters and metered prices produce the invoice; this system's ledger is what the invoice is reconciled against. Both directions of the integration are at-least-once with idempotent effects (10).

---

## 1. Purpose, goals, non-goals

### 1.1 Purpose

Give a product backend one call to make when a customer consumes something (`RecordUsage`), one call to ask before it lets them (`CheckEntitlement`), and a complete, auditable, Polar-backed billing lifecycle behind those two calls: accounts, meters, allowances and caps, billing periods, statements, subscriptions and plan changes, credits, and reconciliation. Prove the whole thing under the fault sweeps and chaos runs the framework is proven under.

### 1.2 Goals

- **The ledger is the truth for usage.** Every unit consumed is one immutable row, attributable to an account, a meter, an instant, and the request that recorded it. Rows are never updated or deleted; corrections are new rows.
- **Exactly once, forever.** A usage event carries a client-chosen ID; the same ID never produces a second row, not within the framework's 24-hour idempotency window and not after it.
- **Entitlement decisions are atomic with recording.** "May this account consume q more units, and if so record them" is one transaction under one row lock, so an enforced cap is never exceeded, on one node or many.
- **Money is exact and explainable.** Every amount on a statement is integer arithmetic over ledger rows and a versioned price mirror, reproducible bit for bit, and reconciled against what Polar charged.
- **Polar state is mirrored, not queried on the hot path.** Subscriptions, periods, credits, and the catalog live in Postgres, fed by Polar webhooks applied exactly once and in order per account. A request never waits on Polar; the only Polar calls in a request path are the ones whose result the caller needs synchronously (a checkout URL), and those run outside any transaction.
- **Operable.** Metrics for the ingest backlog, webhook lag, drift, and cap rejections; a CLI to inspect, replay, and reconcile; runbooks in the form of documented failure analyses.
- **Fast enough.** `RecordUsage` is one short transaction (Appendix E.1) holding one row lock; `CheckEntitlement` is one indexed read; the webhook edge answers in well under Polar's 2-second recommendation.

### 1.3 Non-goals

- Being the merchant of record: tax, payment methods, dunning, refunds, invoice PDFs, and the customer portal are Polar's.
- Tiered, volume, or graduated pricing in v1. The price mirror supports one unit price and one included allowance per meter per plan; tiers are an open question (17.2).
- Multiple currencies. Amounts carry a currency code, the price mirror is per currency, and v1 supports one (USD) per organization.
- Per-seat pricing and seat management. Meters can count seats, but there is no seat entity.
- Sub-second hard caps across nodes without a transaction. Enforcement is transactional and per (account, meter); an application that cannot afford a Postgres round trip per unit batches units (`RecordUsageBatch`) or checks first and records later with a bounded overshoot (5.5).
- Real-time cost display with Polar's rounding. Statements are this system's rating; Polar's invoice may differ by rounding, and reconciliation reports the difference rather than hiding it.
- A UI, emails, or push notifications. The notification table is the contract; delivery is the application's job.
- Aggregations other than sum and count in the entitlement counters. Polar meters may use max, unique, or average; only sum and count meters are mirrored with allowances and caps (17.2).
- Ledger partitioning in v1. The ledger is one table with plain unique indexes; the migration path to range partitions is stated in 5.3 and does not change any invariant.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| Account | The billable party. Keyed by an internal UUID; identified externally by its external ID (a user ID or an organization ID). Owns periods, usage, a subscription, credits, and statements. |
| External ID | The Better Auth `user.id` (user account) or organization ID (organization account). It is also the Polar customer's `external_id`. |
| Principal | The framework's authenticated caller: `Subject` is the user ID, `Tenant` the active organization ID, plus roles and permissions (4.1). |
| Meter | A named quantity the system counts, such as `api_calls` or `tokens_in`. Defines the unit, the aggregation (sum or count), and the Polar event name it is ingested as. |
| Usage event | One `RecordUsage` call: an account, a meter, a quantity, an instant, and a client-chosen event ID. |
| Ledger | The `billing_usage` table: one immutable row per usage event, plus adjustment rows. The source of truth for usage. |
| Counter | The `billing_meter_period` row of an (account, meter, period): the running sum and count of the ledger rows in that period, maintained in the same transaction as the ledger insert. |
| Period | A half-open interval `[starts_at, ends_at)` of an account during which usage is accumulated and rated together. Follows the Polar subscription cycle when the account has a subscription, calendar months (UTC) otherwise. |
| Allowance | The included units of a meter in a period: the plan's included quantity plus credits. |
| Cap | A limit on a meter in a period. A hard cap rejects enforced usage beyond it; a soft cap only notifies. Caps come from the plan (an `overage: block` plan caps at the allowance) and from the account (`SetCaps`). |
| Entitlement | The per (account, meter, period) view that `CheckEntitlement` answers from: allowance, caps, used, remaining, status. |
| Enforce mode | `RecordUsage` with `mode: enforce`: the write is rejected when it would exceed the effective cap. `record` mode always writes. |
| Plan | The mirror of a Polar product: its key, the Polar product ID, the base price, and per meter the included units, unit price, and overage policy. |
| Price version | A monotonic integer on the plan mirror, bumped whenever a price or allowance changes. Statements record the version they were rated with. |
| Subscription mirror | The `billing_subscription` row that mirrors a Polar subscription: status, product, current period, and Polar's `modified_at`. |
| Statement | This system's rating of one closed period: one line per meter with quantity, allowance, billable units, unit price, and amount, plus the total. Not an invoice; Polar's order is the invoice. |
| Credit | Units granted to an account on a meter, from a Polar `meter_credit` benefit or an operator, consumed before billable units. |
| Ingest | The outbound direction: ledger rows sent to Polar's `POST /v1/events/ingest` with `external_id` set to the usage ID so Polar deduplicates. |
| Outbound queue | `billing_polar_outbound`: one row per ledger row to ingest, drained by the ingestor. |
| Webhook | The inbound direction: a Polar event delivered to `POST /billing/webhooks/polar`, verified, stored, and applied by a consumer. |
| Mirror consumer | The durable consumer (`billing_mirror`) that applies stored Polar events to the mirror tables in order per account. |
| Reconciliation | Comparing the ledger and statements with Polar's customer meters and orders, and recording the drift. |
| Drift | A difference reconciliation found: units Polar counted that the ledger did not or the reverse, or an amount that differs beyond the rounding tolerance. |
| Late row | A ledger row whose `occurred_at` falls before the open period and outside the grace period; it lands in the open period with `late = true`. |
| Grace | The state of a period after `ends_at` during which it still accepts rows whose `occurred_at` it contains, before it closes. |
| Fake Polar | `polartest`: an in-process and containerized implementation of the Polar endpoints this system uses, with signed webhook delivery, fault modes, and a state dump for the checkers. |

---

## 3. Architecture

### 3.1 Overview

```
   product backend            Polar (api.polar.sh)               browser (later)
   RecordUsage/Check     webhooks | ingest, customers,           checkout URL, portal URL
        |                        |   checkouts, subscriptions           |
        v                        v            ^                         v
 +------------------------------------------------------------------------------+
 |                               httpapi                                        |
 |  /billing/... operations   /billing/webhooks/polar (raw, Standard Webhooks)  |
 +------------------------------+-----------------------------------------------+
                                |  Send / Publish / Stream
 +------------------------------v-----------------------------------------------+
 |                        mediator (standard chain)                             |
 |  recovery tracing logging metrics timeout authz ratelimit validation cache   |
 |  retry unitofwork idempotency cache-invalidation | handlers of package billing|
 +----+----------------------+------------------------------+-------------------+
      |                      |                              |
      v                      v                              v
 +-----------+   +------------------------+   +-------------------------------+
 | ledger    |   | mirror tables          |   | outbox (framework)            |
 | counters  |   | account, subscription, |   | billing.* topics              |
 | periods   |   | plan, credit, polar    |   +---------------+---------------+
 | entitle.  |   | event, reconciliation  |                   | relay -> Redis
 | statements|   +------------------------+                   v
 | outbound  |                                +-------------------------------+
 +-----+-----+                                | consumers (framework)         |
       |                                      | billing_mirror  billing_sync  |
       | ingestor (component)                 | billing_entitlements          |
       v                                      | billing_notify billing_stmts  |
 POST /v1/events/ingest                       | billing_reconcile billing_audit|
 (external_id = usage_id)                     +-------------------------------+
```

Postgres is the source of truth for everything this system owns; the framework's outbox, inbox, and idempotency tables make its events and commands exactly-once in effect (spec 10, G4, G7, G8). Polar is the source of truth for money; its state is mirrored through webhooks and verified through reconciliation. Redis carries events between nodes and caches read models; losing it costs latency, never data (spec 7.7).

Three kinds of work, three places:

| Work | Where | Why |
|---|---|---|
| Record and decide | command handlers, one transaction | atomicity with the ledger (13, B3, B4) |
| Derive and mirror | durable consumers | off the request path, exactly-once effect, ordered per account |
| Talk to Polar | components (ingestor, reconciler) and two consumers (`billing_sync`, `billing_reconcile`) | at-least-once with idempotent operations; never inside a request transaction (B11) |

### 3.2 Repository layout

```
go-api-backend/
  spec-billing.md                this document
  billing/                       the module: requests, events, handlers, consumers, register, schema access
  billing/entitle/               pure: period assignment, cap decisions, threshold crossings (100 percent coverage)
  billing/rating/                pure: statements from ledger rows, allowances, credits, and a price mirror (100 percent)
  billing/mirror/                pure: apply one Polar event to mirror state; reorder and duplicate safe (100 percent)
  billing/polar/                 Polar API client, wire types, decimal parsing, Standard Webhooks verification (100 percent for verify and codec)
  billing/polar/polartest/       fake Polar server, scripted webhook emitter, fault modes, state dump, conformance suite
  billing/hook/                  the webhook edge: raw-body http.Handler that verifies and Sends ApplyPolarEvent
  billing/ingest/                PolarIngestor component: drains billing_polar_outbound in batches
  billing/periods/               PeriodCloser component: grace and close, opens calendar periods
  billing/reconcile/             Reconciler component: daily drift check of open periods
  billing/janitor/               billing janitor: outbound and event payload retention
  billing/auth/                  Better Auth JWT verification (JWKS, EdDSA and ES256; HS256 for tests) to authz.Principal
  billing/migrations/            NNNN_name.sql, embedded, with -- down sections (6.9)
  billing/testkit/               billing invariants IB1 to IB8, workload helpers shared by sweep and chaos
  cmd/billingd/                  the service process (3.4)
  cmd/billingctl/                operations CLI (12.3)
  cmd/polarfake/                 the fake Polar as a container for compose and chaos
  api/billing-openapi.json       committed OpenAPI document, drift-checked in CI
  test/faultsweep/sweep_billing_*_test.go   billing sweep scenarios in the framework's sweep package (14.4)
  test/chaos/billing_*.go        billing workloads, nemeses, and checkers (14.6)
  test/integration/billing_*_test.go        end-to-end scenarios of the service process (14.5)
  deploy/docker-compose.yml      profile billing (billingd, polarfake); profile chaos gains polarfake behind Toxiproxy
```

Package names import as `billing`, `entitle`, `rating`, `mirror`, `polar`, `polartest`, `hook`, `ingest`. The pure packages import nothing of `pg` or `redisx`; the handlers in `billing` are thin: SQL, one call into a pure package, one publish.

### 3.3 Dependencies

No dependency is added to `go.mod` beyond what the framework has:

| Need | Choice | Why |
|---|---|---|
| Polar API | own client in `billing/polar` over `net/http` | Nine endpoints are used (10.1); a hand-written client is smaller than the SDK, fault-pointed at every call, and its wire types are pinned by sandbox fixtures |
| Webhook verification | own, in `billing/polar` | Standard Webhooks is HMAC-SHA256 over three strings (Appendix F); no library needed |
| JWT verification | own, in `billing/auth`, over `crypto/ed25519`, `crypto/ecdsa`, `crypto/hmac` | Better Auth's JWT plugin signs with EdDSA by default and publishes a JWKS; the example service's HS256 verifier is kept for tests |
| Big integer arithmetic | `math/big` | Rating multiplies quantities by prices in 10^-12 of a cent; the product can exceed `int64` (5.7) |
| Decimal parsing | own, in `billing/polar` | Polar's metered `unit_amount` is a decimal string in cents with up to twelve decimals; it is parsed to an integer in 10^-12 of a cent exactly and rejected when finer (5.7) |

### 3.4 Runtime components and lifecycle

`cmd/billingd` composes one `mediator.Runtime` (spec 3.4):

| Component | Slot | Role |
|---|---|---|
| `httpapi.Listener` | `HTTP` | The operations under `/billing`, the webhook mount, health, docs |
| `redisx.Consumers` | `Consumers` | The seven billing consumer groups (8.3) |
| `pg.Relay` | `Relay` | Outbox to Redis for the billing topics |
| `redisx.RemoteServer`, `redisx.ReplyReader` | `RemoteServer`, `ReplyReader` | Enabled so that a second service can `Send` billing commands remotely (spec 7.6); not used by billing itself |
| `pg.Janitor` | `Janitor` | Framework retention |
| `ingest.PolarIngestor` | `Extra` | Drains the outbound queue to Polar (9.1) |
| `periods.PeriodCloser` | `Extra` | Grace and close periods, open calendar periods (9.2) |
| `reconcile.Reconciler` | `Extra` | Daily drift check (9.3) |
| `janitor.Billing` | `Extra` | Billing retention (9.4) |

Shutdown order is the runtime's: HTTP stops accepting (the webhook edge included, so Polar retries what it could not deliver), the remote server stops, consumers finish the current message and acknowledge after commit, the extras stop with the consumers (the ingestor after the current batch is marked), the relay after the current batch, leases are released, pools close. Every component exposes `Healthy()`; `GET /billing/readyz` adds the ingest backlog and webhook apply lag to the framework's checks (12.2).

The service is stateless across restarts: everything a component needs to resume is in Postgres (outbound queue state, period state, the raw Polar events) or is recomputed (the ingestor's batch, the closer's due list).

### 3.5 Framework changes

Two additive generalizations are needed. Each is recorded in `docs/design-notes.md` section 6 before it is implemented, keeps every existing call site and test unchanged, and is covered by unit rows in the framework package it touches.

| # | Change | Why the existing API is not enough | Shape |
|---|---|---|---|
| F1 | `httpapi.Config.Mounts []httpapi.Mount` where `Mount{Pattern string; Handler http.Handler}` is registered on the server's mux after the operations and the health and docs routes, not decoded, not documented in OpenAPI, and not subject to `MaxBodyBytes` (the handler bounds its own body) | The webhook edge must read the raw body to verify the signature, and `Listener` serves `*Server` only; there is no way to put a raw handler beside the operations. The alternative, an `Authenticator` that verifies the signature and rewrites `r.Body` into the request struct's shape, was rejected because an authenticator that changes the body is a trap for the next reader | A pattern that conflicts with an operation is a `New` error; `BuildCheck` is unchanged because mounts are not requests; the drain signal of the listener reaches mounted handlers through the request context like every other route |
| F2 | `pg.Migrator{FS fs.FS; VersionTable, LockName string}` with `Up`, `Down`, `Status`; `pg.Migrate`, `pg.MigrateDown`, `pg.MigrationStatus` become calls on a package-level migrator with the framework's values | `pg.Migrate` reads the framework's embedded FS and `mediator_schema_version`; billing needs its own versioned, append-only migrations with the same up-down-up test, and idempotent DDL (`examples/orders`) cannot alter a production schema | `SetMigrationsFS` (a test seam) keeps working by swapping the package-level migrator's FS; the version table and lock name are validated against `mediator.NamePattern` |

`mediator.Runtime.Extra` already stops the extra components with the consumers, and `Runtime.Healthy()` already aggregates their `Healthy()` (the example service passes it to `ReadyChecks`), so readiness needs no framework change. Nothing else in the framework changes. In particular the behavior order, the idempotency protocol, the outbox and consumer protocols, and every existing fault point are used as they are; the new fault points of Appendix A join the shared catalogue.

---

## 4. Identity, tenancy, and authorization

### 4.1 Principals

The authenticator in `billing/auth` verifies a Bearer JWT and maps it to `authz.Principal`:

| Claim | Principal field | Notes |
|---|---|---|
| `sub` | `Subject` | The Better Auth user ID. Required. |
| `org` (or the configured claim name, default `activeOrganizationId`) | `Tenant` | The active organization ID when the organizations plugin is in use. Optional. |
| `roles` | `Roles` | Strings. `billing_admin` is the operator role (4.3). |
| `permissions` | `Permissions` | Strings. `billing:usage:write` marks a service principal (4.3). |
| everything else | `Claims` | Kept for handlers that need it; never logged. |

Verification: the key comes from a JWKS URL (`AUTH_JWKS_URL`, Better Auth's `/api/auth/jwks`) cached for `AUTH_JWKS_TTL` (default 10 minutes) and refreshed once on an unknown `kid`; algorithms EdDSA (Ed25519) and ES256 are accepted in production, HS256 only when `AUTH_JWT_HS256_SECRET` is set (tests and local runs, with the same warning the example service logs); `iss` and `aud` are checked when configured; `exp` and `nbf` are checked with a 60-second leeway. Every failure is `CodeUnauthorized` with a fixed client message; the cause is logged with the correlation ID. The `alg` of the token must match the key's algorithm; `none` and algorithm confusion (an HS256 token against a public key) are rejected by construction and by test.

Service principals: a product backend that records usage authenticates with a JWT whose `permissions` claim contains `billing:usage:write` (minted by the auth server for the service, or by a static signing key configured for machine clients). There is no API key store in v1; a static key would be a second credential system.

Reserved claims: only the webhook edge (10.4) constructs `Principal{Subject: "polar", Permissions: ["billing:webhook"]}`, after verifying the signature, and only consumers and components construct the system principal (`billing.SystemContext`). No token can carry `billing:webhook` or `billing_system`; the authenticator strips them from any token that presents them and logs a warning (`TestAuth_StripsReservedClaims`).

### 4.2 Accounts and ownership

An account is either a user account (external ID = user ID, `kind = user`) or an organization account (external ID = organization ID, `kind = org`). The account a principal acts for is resolved in one way everywhere (`billing.AccountFor(ctx)`, used by every self-service handler):

1. If `Principal.Tenant` is set, the account is the organization account with that external ID.
2. Else the user account with external ID `Principal.Subject`.
3. A `billing_admin` may name any account explicitly: every account-scoped request has an `accountId` field; for non-admins it must equal the resolved account or the request is `CodeForbidden`.

Ownership is checked in the handler after the row is loaded (spec 5.7, resource-level checks): `billing.checkOwner(ctx, account)` returns nil for the owner or an admin and `CodeForbidden` otherwise. A non-owner gets 403, not 404: account IDs are UUIDv7 values this system generates, so 403 leaks nothing that matters, and one code keeps the tests small.

### 4.3 Requirements per request

Every request implements `Requires()` (the service builds with `RequireAuthByDefault: true`, spec 5.7). The classes:

| Class | Requirement | Who satisfies it |
|---|---|---|
| Self-service | `authz.Authenticated()` plus the ownership check in the handler | The account owner (user or organization member), any admin |
| Service | `authz.Permission("billing:usage:write")` | The product backend's service principal; admins do not get it by role |
| Admin | `authz.Role("billing_admin")` | Operators |
| Webhook | `authz.Permission("billing:webhook")` | The webhook edge only |
| System | `authz.Role("billing_system")` | Consumers and components, which attach the system principal before a nested `Send`; unreachable over HTTP by construction (4.1) and by test (`TestSystemRequests_UnreachableOverHTTP` sends every system request with an admin token and expects 403) |

Appendix B lists the class of every request.

### 4.4 Organizations and members

An organization account is billed for the usage its members generate. `RecordUsage` carries the account, not the member; the member's user ID, when the caller knows it, goes in `attributes.member` and is forwarded to Polar as `external_member_id` (10.2). The system does not enforce membership: the auth server does, by issuing a token whose `org` claim is an organization the user belongs to.

---

## 5. Domain model and rules

### 5.1 Accounts

An account is the billable party. Its row (`billing_account`, 6.1) carries the internal ID, the kind and external ID, the email Polar needs to create a customer, the currency, the status, and the Polar customer ID once linked.

Lifecycle:

| From | Command | To | Effect |
|---|---|---|---|
| none | `OpenAccount` | `active` | Row, first calendar period opened, `AccountOpened` published; the Polar customer is created asynchronously (5.1.1) |
| `active` | `SuspendAccount` | `suspended` | Enforced usage is rejected with `CodeForbidden` (reason `account_suspended`); record-mode usage is still written because it happened; reads work; ingest continues |
| `suspended` | `ReinstateAccount` | `active` | |
| `active`, `suspended` | `CloseAccount` | `closed` | The open period moves to grace at once, every usage is rejected, ingest of already recorded rows continues, the Polar subscription is revoked through `CancelSubscription{Revoke: true}` if one is active |

Every transition writes `billing_audit_log` in the same transaction through the in-process handler of `AccountChanged` (8.1). Every account has exactly one open period at all times (5.4); `OpenAccount` opens the first one.

#### 5.1.1 The Polar customer

`OpenAccount` does not call Polar: the customer is created by the `billing_sync` consumer from `AccountOpened` with `POST /v1/customers/` and `external_id` set to the account's external ID (10.5), then linked through `LinkPolarCustomer`. Until the link exists, `billing_account.polar_customer_id` is NULL and ledger rows wait in the outbound queue in state `waiting_customer` (9.1). A conflict on `external_id` or `email` (Polar answers 422) is resolved by `GET /v1/customers/external/{external_id}`: the existing customer is linked. That is what makes the Better Auth plugin and this backend coexist: whichever created the customer first, the other finds it.

### 5.2 Meters

A meter is a named quantity: `meter_key` matching `^[a-z][a-z0-9_]{0,63}$`, a display name, a unit label, an aggregation (`sum` or `count`), the Polar event `name` it is ingested as (NULL means not ingested, for internal meters), the Polar meter ID it maps to (from the catalog sync, 10.7), and a status (`active`, `retired`).

Meters are configuration as data: seeded by migration 0001 with the initial set and changed by `UpsertMeters` (admin, `billingctl meters apply`). Rules:

- A `count` meter accepts only `quantity = 1` per event; a `sum` meter accepts `1 <= quantity <= 10^12`.
- Retiring a meter keeps its rows and counters; new usage on it is `CodePrecondition` with reason `meter_retired`; an unknown meter key is `CodeNotFound`.
- Renaming a meter key is not supported; the key is persisted in the ledger. Change the display name.
- Changing `polar_event_name` affects only rows recorded after the change; already queued outbound rows carry the name they were queued with.

### 5.3 The ledger

`billing_usage` holds one row per usage event and one per adjustment (6.1). Rules:

- **Append-only.** A trigger rejects every `UPDATE` and `DELETE` with SQLSTATE `BL001`. There is no operator path around it; a wrong row is corrected by an adjustment row (`AdjustUsage`) that references it.
- **Identity.** `usage_id` is a UUIDv7 generated by the handler from the mediator clock, so ascending `usage_id` is ascending recording time within a node and the cursor of `ListUsageEvents` and `WatchUsage`.
- **Dedup key.** `(account_id, source_event_id)` is unique. For API rows `source_event_id` is the caller's `eventId`; for adjustments it is the `adjustmentId`. The row stores `request_hash`, the canonical hash of the recording request (`mediator.CanonicalHash`, the same function the idempotency behavior uses), so a replay after the framework's 24-hour idempotency window is still recognized: equal hash means the same event (`deduplicated: true`, the original `usage_id`, and the counters as they are now), different hash means `CodeIdempotencyMismatch`. Within the window the framework replays the original response byte for byte and the handler never runs.
- **Time.** `occurred_at` is the caller's instant (default: now) and must lie in `[now - BILLING_OCCURRED_AT_PAST, now + BILLING_OCCURRED_AT_FUTURE]` (defaults 72 hours and 5 minutes) for API rows; `recorded_at` is the server's instant. Adjustments may carry any `occurred_at` not before the account was opened.
- **Period.** Every row belongs to exactly one period, assigned at write time by `entitle.AssignPeriod` (5.4); `late` records that the row's instant fell before its period.
- **Witness columns.** `used_after` is the counter of the row's (account, meter, period) immediately after the row was added, and `cap_at_write` is the effective cap the enforce decision used (NULL in record mode or when unlimited). They make the linearizability and cap checks of section 14 possible from the ledger alone.
- **Attributes.** `attributes` is a flat string map, at most 20 keys of at most 40 characters with values of at most 200 characters, validated by tag rules and a `Validate` hook. Keys are forwarded to Polar as event metadata (10.2); the reserved keys `meter`, `quantity`, `usage_id`, `period_id`, `account_id`, and `mode` are rejected because the ingest mapping sets them.
- **Provenance.** `request_id`, `correlation_id`, `subject` (the recording principal), and `node` are recorded for every row.
- **Partitioning path.** v1 is one table. When the ledger grows past the point where index maintenance dominates (the trigger is operational, not a number in this document), a later migration adds `billing_usage_key (account_id, source_event_id, usage_id)` as the dedup index and turns `billing_usage` into a range-partitioned table by `recorded_at` month, with the janitor creating partitions two months ahead. The handler gains one insert; no invariant, request, or response changes.

### 5.4 Periods

A period is a half-open interval `[starts_at, ends_at)` of one account. States:

```
open --(ends_at reached, or a subscription boundary moved)--> grace --(ends_at + grace elapsed, or forced)--> closed
```

Rules:

- **Exactly one open period per account, at most one in grace.** Enforced by partial unique indexes and checked by IB4.
- **Contiguous, non-overlapping.** The periods of an account, sorted by `starts_at`, chain end to start. There are no gaps: usage at any instant after the account opened maps to one period.
- **Calendar periods** (`kind = calendar`) cover `[first of month 00:00 UTC, first of next month)`. They apply while the account has no active subscription.
- **Subscription periods** (`kind = subscription`) mirror `[current_period_start, current_period_end)` of the account's active subscription (5.9).
- **Boundary changes from Polar.** Whenever the mirror consumer applies an event that changes the active subscription's current period, the period table follows in the same transaction: the open period's `ends_at` becomes the new `current_period_start` (shrinking or extending it, never overlapping), it moves to grace, and a new open period starts at that instant. The first `subscription.active` does the same to the calendar period it interrupts, and `subscription.revoked` (or the end of a canceled subscription) does it in reverse: the subscription period ends at the subscription's end and a calendar period opens there.
- **Grace and close.** `PeriodCloser` (9.2) moves an open period to grace when `now >= ends_at` for calendar periods (subscription periods move at the boundary event; if the event is late, the closer moves them too, at `ends_at + BILLING_BOUNDARY_SLACK`, default 6 hours, and opens the next subscription period from the mirror's expected cycle) and closes a grace period when `now >= ends_at + BILLING_LATE_GRACE` (default 72 hours) by sending `ClosePeriod`. Closing freezes the allowance snapshot (5.5) and publishes `PeriodClosed`, which the statements consumer rates (5.7).
- **Row assignment** (`entitle.AssignPeriod(occurredAt, open, grace)`): the grace period when it contains the instant; the open period when it contains the instant; the open period with `late = true` when the instant is before the open period and not in grace; the open period with `late = false` when the instant is at or after `open.ends_at` (possible for up to `BILLING_OCCURRED_AT_FUTURE` at a boundary; documented, bounded, and counted by `billing.usage.early`).

### 5.5 Entitlements, caps, and enforcement

`billing_entitlement` has one row per (account, meter, period) with the limits that apply there:

| Field | Source |
|---|---|
| `included` | the plan mirror's included units for the meter at the row's price version, or 0 once a Polar grant row (5.6) is bound to this period for the meter: the plan's figure is what Polar grants each cycle, and once the grant is mirrored the credit row carries those units |
| `credits` | the sum of `remaining` over the account's credit rows on the meter that are usable in this period (5.6): operator credits, the Polar grant bound to this period, and the rollover remainders of earlier grants |
| `overage` | the plan's policy for the meter: `charge` (a metered price exists; usage beyond the allowance is billed) or `block` (no price; the allowance is the cap) |
| `hard_cap`, `soft_cap` | the account's own caps from `billing_cap` (`SetCaps`), NULL when unset |
| `price_version` | the plan mirror version used |

The rows are recomputed by `RefreshEntitlements` (a system command) whenever the inputs change: the entitlements consumer sends it on `SubscriptionChanged`, `CreditGranted`, and `PeriodOpened`; `SetCaps` updates the two cap columns directly and bumps the account's cache tags. An account whose meter has no row yet gets the plan defaults lazily (the free plan's when there is no subscription).

The decision is pure (`entitle.Decide(limits, used, qty) Decision`):

```
allowance      = included + credits
cap            = min over the defined values of { hard_cap, allowance when overage == block }   (absent means unlimited)
allowed        = cap is absent || used + qty <= cap
remaining      = cap - used when cap is present, -1 otherwise
status         = hard_exceeded when !allowed
               | soft_exceeded when (soft_cap present && used + qty > soft_cap) || (overage == charge && used + qty > allowance)
               | ok
crossings      = every threshold t in {50, 80, 100} percent of allowance, and of hard_cap when present,
                 with used < t <= used + qty   (only when allowed)
```

`RecordUsage` in enforce mode locks the counter row, calls `Decide`, and either writes (Appendix E) or returns `CodePrecondition` with `Details{reason: "quota_exceeded", used, cap, remaining}`. The lock serializes every enforced write of one (account, meter, period), on every node, so a cap is never exceeded by enforced writes (B4).

Record mode writes regardless and reports the status. `CheckEntitlement` reads the counter without a lock and answers `Decide` for a hypothetical quantity; a caller that checks and then records in record mode can overshoot by whatever its own concurrency admits. That bound is the caller's, and the response says so (`advisory: true`).

A suspended account fails every enforce-mode write with `CodeForbidden` before the counter is touched.

### 5.6 Credits

A credit is units of one meter granted to one account: from a Polar `meter_credit` benefit (one row per grant and cycle, mirrored from `benefit_grant.created` and `benefit_grant.cycled`, keyed by the grant ID and the period the row is bound to) or from an operator (`GrantCredit`, keyed by the idempotency header). Each credit has `units`, `remaining`, `rollover`, `granted_at`, an optional `expires_at`, and a `period_id` when it is bound to one period. A Polar grant is bound to the account's period containing `granted_at` (the mirror re-binds it when a later `subscription.cycled` moves the boundary to before `granted_at`); Polar grants it at the start of each cycle and, unless the benefit rolls over, it expires at the cycle's end.

A Polar grant carries the same units the plan mirror lists as `included_units` for the meter (5.8). To count them once, the entitlement uses the plan's figure as `included` only until the period's grant row exists, and 0 afterwards, when the row is counted in `credits` (5.5, `PropAllowance_NoDoubleCount`). The free plan has no Polar grants, so its included units are always the plan's.

Credits count toward the allowance during the period (5.5) and are consumed at rating (5.7): `remaining` is reduced FIFO by `granted_at` when a statement is finalized, never during the period. A rollover grant keeps its `remaining` past the period it was bound to and is usable in the account's later periods until `expires_at`; a non-rollover grant is usable only in its period. Polar keeps its own balance (`active_meters[].balance` in the customer state); the mirror is for enforcement and estimates, and reconciliation compares the two (10.8).

### 5.7 Money and rating

Amounts are integers in the currency's minor unit (cents for USD) stored as `BIGINT`. Unit prices are integers in 10^-12 of a minor unit (`unit_amount_pico`), because Polar's metered `unit_amount` is a decimal string in cents with up to twelve decimals (checked against the API reference on 2026-09-27; pinned by the sandbox fixture at BM2, 17.2 question 3): `polar.ParseAmount("0.0015")` returns `1500000000`, and a string with more than twelve decimals, a sign, or an exponent is an error. Polar's schema admits at most five integer digits, so a unit price is below 10^17 and the column never overflows; the products in rating can exceed `int64`, which is why they are computed in `math/big`. `cap_amount` and fixed prices are integers in cents and need no parsing.

A statement is the rating of one closed period, computed by `rating.Build(in Input) (Statement, error)` where `Input` is the period's ledger rows, the frozen allowance snapshot, the credits usable in the period, and the plan mirror at the recorded price version. Per meter, with all arithmetic in `math/big`:

```
quantity        = sum of quantity over the period's rows of the meter (adjustments included; may be negative)
gross           = max(0, quantity)
credits_applied = min(credits_available, max(0, gross - included))
billable        = max(0, gross - included - credits_applied)
amount_pico     = billable * unit_amount_pico                (0 when the meter has no price: overage block)
amount_minor    = round half up of amount_pico / 10^12, then min with cap_amount_minor when the price has a cap
```

The base fee of the plan is one line (`kind = base`, informational; Polar bills it). `total_minor` is the sum of the lines. Rows are processed sorted by `usage_id`, credits sorted by `granted_at` then `credit_id`, lines sorted by `meter_key`; the output is canonical JSON and `checksum` is its SHA-256. Building the same input twice is byte-identical (B8, property `PropRating_Deterministic`). Rounding happens once per line, never per row, so a statement never differs from the exact rational amount by more than half a minor unit per line; reconciliation uses that as its tolerance (10.8).

### 5.8 Plans and the catalog mirror

A plan mirrors one Polar product (`billing_plan`) with its per-meter terms (`billing_plan_meter`). Rows come from the catalog sync (10.7) only; nobody edits prices here.

| Plan field | From Polar |
|---|---|
| `plan_key` | the product's `metadata.plan_key`; a product without it is not a plan and is ignored with a warning (`billing.catalog.ignored`) |
| `polar_product_id`, `name`, `is_recurring`, `recurring_interval` | the product |
| `base_amount_minor`, `currency` | the product's fixed price, if any |
| per meter: `polar_price_id`, `unit_amount_pico`, `cap_amount_minor` | the product's metered price whose `meter_id` maps to a billing meter's `polar_meter_id` |
| per meter: `included_units`, `rollover` | the product's `meter_credit` benefit for that meter (`properties.units`, `properties.rollover`): the units Polar grants each cycle, which the mirrored grant rows carry once they arrive (5.6) |
| `overage` per meter | `charge` when a metered price exists, `block` otherwise |
| `version` | bumped whenever any of the above changes; earlier versions are kept for statements |

The free plan (`plan_key = free`, no product) is seeded by migration 0001 (6.1) with `source = manual`, the included units listed there, and `overage = block` on every meter; its allowances change only through a later migration that inserts a new version; it is the plan of every account without an active subscription.

### 5.9 The subscription mirror

`billing_subscription` mirrors one Polar subscription: ID, account, customer, product and plan, status (Polar's enum: `incomplete`, `incomplete_expired`, `trialing`, `active`, `past_due`, `canceled`, `unpaid`, `paused`), `current_period_start`, `current_period_end`, `cancel_at_period_end`, the timestamps Polar sends, `polar_modified_at`, and the webhook that last changed it.

Rules the mirror consumer applies (`mirror.Apply(state, event) (state, actions, error)` is pure and the consumer executes the actions):

- **Monotonic by `modified_at`.** An event whose `data.modified_at` is older than the stored `polar_modified_at` is recorded as `stale` and not applied. An equal `modified_at` is applied in delivery order, which is per-account order through the outbox; equal instants only arise from Polar's own retries, whose bodies are identical.
- **Account resolution.** The account is found by the customer's external ID in the payload (`data.customer.external_id` for subscriptions, orders, and benefit grants; `data.external_customer_id` or `data.customer.external_id` for checkouts; `data.external_id` for customer events). An event whose external ID matches no account is stored and marked `unmatched`; it is not retried by the consumer. `billingctl webhooks reapply` re-publishes it once the account exists.
- **Entitlement-relevant transitions** (`active`, `trialing`, `cycled`, `updated` with a product or period change, `canceled`, `uncanceled`, `revoked`, `past_due`, `paused`, `resumed`) publish `SubscriptionChanged` and move the account's periods (5.4). `past_due` and `unpaid` do not suspend the account by themselves: whether to keep serving a customer whose payment failed is a product decision, so the status is mirrored, `SubscriptionChanged` carries it, and the notifications consumer records it; an operator or the application decides.
- **One active subscription per account** is the expectation. Polar allows more; when two are active the account's current subscription is the one with the latest `current_period_start`, the other is mirrored but ignored for periods, and `billing.subscription.multiple` counts it.

### 5.10 Notifications

`billing_notification` records what the application should tell the customer: threshold crossings at 50, 80, and 100 percent of the allowance and of the hard cap, `cap_reached` when an enforced write was rejected (written by a nested `RecordNotification` in its own transaction, since the rejecting one rolls back; 7.2.2), and the subscription conditions `past_due`, `payment_failed`, `canceled`, `revoked`. The unique key `(account_id, period_id, meter_key, kind)`, with nulls compared as equal, makes each at most one per period and each subscription condition at most one per account. The application polls `ListNotifications` or reads the table; `delivered_at` is the application's to set. Nothing here sends email.

### 5.11 Audit

Two records:

- `billing_audit_log`: one row per operator or customer change of an account, its caps, its credits, the meters, or the catalog, written by the in-process handler of the corresponding notification in the transaction of the command (actor, action, before, after as JSON, correlation ID). Like the example service's `AuditLogger`, it commits or rolls back with the command.
- `billing_event_log`: one row per durable billing event, written by the `billing_audit` consumer with the envelope and payload. The framework's outbox keeps seven days; this table keeps the history.

---

## 6. Schema

All billing tables carry the `billing_` prefix and live beside the framework's `mediator_` tables in the same database. DDL is in `billing/migrations`, one file per milestone, applied by the billing migrator (6.9).

### 6.1 Migration 0001, core (BM1)

```sql
CREATE TABLE billing_account (
    account_id        UUID        PRIMARY KEY,
    kind              TEXT        NOT NULL CHECK (kind IN ('user', 'org')),
    external_id       TEXT        NOT NULL UNIQUE,
    email             TEXT        NOT NULL,
    name              TEXT,
    currency          TEXT        NOT NULL DEFAULT 'USD',
    status            TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'closed')),
    polar_customer_id TEXT        UNIQUE,
    sync_state        TEXT        NOT NULL DEFAULT 'pending' CHECK (sync_state IN ('pending', 'linked', 'failed')),
    sync_error        TEXT,
    sync_fence        BIGINT      NOT NULL DEFAULT 0,   -- fencing token of the consumer that linked the customer (8.3)
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    version           BIGINT      NOT NULL DEFAULT 1
);

CREATE TABLE billing_meter (
    meter_key        TEXT        PRIMARY KEY CHECK (meter_key ~ '^[a-z][a-z0-9_]{0,63}$'),
    name             TEXT        NOT NULL,
    unit             TEXT        NOT NULL,
    aggregation      TEXT        NOT NULL CHECK (aggregation IN ('sum', 'count')),
    polar_event_name TEXT        CHECK (polar_event_name IS NULL OR length(polar_event_name) BETWEEN 1 AND 128),
    polar_meter_id   TEXT        UNIQUE,
    status           TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'retired')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE billing_plan (
    plan_key           TEXT        NOT NULL,
    version            INT         NOT NULL,
    polar_product_id   TEXT,
    name               TEXT        NOT NULL,
    is_recurring       BOOLEAN     NOT NULL,
    recurring_interval TEXT,
    base_amount_minor  BIGINT      NOT NULL DEFAULT 0,
    currency           TEXT        NOT NULL DEFAULT 'USD',
    status             TEXT        NOT NULL CHECK (status IN ('active', 'archived')),
    source             TEXT        NOT NULL CHECK (source IN ('catalog', 'manual')),
    synced_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (plan_key, version)
);
CREATE UNIQUE INDEX billing_plan_product_idx ON billing_plan (polar_product_id, version) WHERE polar_product_id IS NOT NULL;
CREATE TABLE billing_plan_current (
    plan_key TEXT PRIMARY KEY,
    version  INT  NOT NULL,
    FOREIGN KEY (plan_key, version) REFERENCES billing_plan (plan_key, version)
);

CREATE TABLE billing_plan_meter (
    plan_key          TEXT   NOT NULL,
    version           INT    NOT NULL,
    meter_key         TEXT   NOT NULL REFERENCES billing_meter (meter_key),
    included_units    BIGINT NOT NULL DEFAULT 0,
    rollover          BOOLEAN NOT NULL DEFAULT false,
    unit_amount_pico  BIGINT,                      -- NULL: no metered price, overage block
    cap_amount_minor  BIGINT,
    polar_price_id    TEXT,
    PRIMARY KEY (plan_key, version, meter_key),
    FOREIGN KEY (plan_key, version) REFERENCES billing_plan (plan_key, version)
);

CREATE TABLE billing_period (
    period_id       UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL REFERENCES billing_account (account_id),
    kind            TEXT        NOT NULL CHECK (kind IN ('calendar', 'subscription')),
    subscription_id TEXT,
    starts_at       TIMESTAMPTZ NOT NULL,
    ends_at         TIMESTAMPTZ NOT NULL CHECK (ends_at > starts_at),
    status          TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'grace', 'closed')),
    grace_at        TIMESTAMPTZ,
    closed_at       TIMESTAMPTZ,
    plan_key        TEXT        NOT NULL,
    price_version   INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX billing_period_open_idx  ON billing_period (account_id) WHERE status = 'open';
CREATE UNIQUE INDEX billing_period_grace_idx ON billing_period (account_id) WHERE status = 'grace';
CREATE UNIQUE INDEX billing_period_start_idx ON billing_period (account_id, starts_at);
CREATE INDEX billing_period_due_idx ON billing_period (ends_at) WHERE status IN ('open', 'grace');

-- Allowance snapshot frozen at close; rating reads this, never billing_entitlement.
CREATE TABLE billing_period_allowance (
    period_id      UUID   NOT NULL REFERENCES billing_period (period_id),
    meter_key      TEXT   NOT NULL REFERENCES billing_meter (meter_key),
    included       BIGINT NOT NULL,
    credits        BIGINT NOT NULL,
    overage        TEXT   NOT NULL CHECK (overage IN ('charge', 'block')),
    hard_cap       BIGINT,
    soft_cap       BIGINT,
    price_version  INT    NOT NULL,
    PRIMARY KEY (period_id, meter_key)
);

CREATE TABLE billing_usage (
    usage_id        UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL REFERENCES billing_account (account_id),
    meter_key       TEXT        NOT NULL REFERENCES billing_meter (meter_key),
    period_id       UUID        NOT NULL REFERENCES billing_period (period_id),
    quantity        BIGINT      NOT NULL CHECK (quantity <> 0),
    occurred_at     TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    late            BOOLEAN     NOT NULL DEFAULT false,
    mode            TEXT        NOT NULL CHECK (mode IN ('enforce', 'record')),
    source          TEXT        NOT NULL CHECK (source IN ('api', 'adjustment', 'import')),
    source_event_id TEXT        NOT NULL CHECK (length(source_event_id) BETWEEN 1 AND 200),
    request_hash    BYTEA       NOT NULL,
    attributes      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    used_after      BIGINT      NOT NULL,
    cap_at_write    BIGINT,
    adjusts         UUID        REFERENCES billing_usage (usage_id),
    reason          TEXT,
    request_id      UUID        NOT NULL,
    correlation_id  TEXT        NOT NULL,
    subject         TEXT        NOT NULL,
    node            TEXT        NOT NULL,
    CONSTRAINT billing_usage_positive CHECK (source = 'adjustment' OR quantity > 0),
    CONSTRAINT billing_usage_dedup UNIQUE (account_id, source_event_id)
);
CREATE INDEX billing_usage_period_idx  ON billing_usage (account_id, meter_key, period_id, usage_id);
CREATE INDEX billing_usage_account_idx ON billing_usage (account_id, usage_id);

CREATE FUNCTION billing_usage_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'billing_usage is append-only' USING ERRCODE = 'BL001';
END $$;
CREATE TRIGGER billing_usage_immutable BEFORE UPDATE OR DELETE ON billing_usage
    FOR EACH ROW EXECUTE FUNCTION billing_usage_immutable();

CREATE TABLE billing_meter_period (
    account_id    UUID   NOT NULL,
    meter_key     TEXT   NOT NULL,
    period_id     UUID   NOT NULL REFERENCES billing_period (period_id),
    quantity      BIGINT NOT NULL DEFAULT 0,
    events        BIGINT NOT NULL DEFAULT 0,
    last_usage_id UUID,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, meter_key, period_id)
);

CREATE TABLE billing_cap (
    account_id UUID   NOT NULL REFERENCES billing_account (account_id),
    meter_key  TEXT   NOT NULL REFERENCES billing_meter (meter_key),
    hard_cap   BIGINT CHECK (hard_cap IS NULL OR hard_cap >= 0),
    soft_cap   BIGINT CHECK (soft_cap IS NULL OR soft_cap >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, meter_key)
);

CREATE TABLE billing_entitlement (
    account_id    UUID   NOT NULL,
    meter_key     TEXT   NOT NULL,
    period_id     UUID   NOT NULL REFERENCES billing_period (period_id),
    included      BIGINT NOT NULL,
    credits       BIGINT NOT NULL,
    overage       TEXT   NOT NULL CHECK (overage IN ('charge', 'block')),
    hard_cap      BIGINT,
    soft_cap      BIGINT,
    price_version INT    NOT NULL,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, meter_key, period_id)
);

CREATE TABLE billing_credit (
    credit_id              UUID        PRIMARY KEY,
    account_id             UUID        NOT NULL REFERENCES billing_account (account_id),
    meter_key              TEXT        NOT NULL REFERENCES billing_meter (meter_key),
    units                  BIGINT      NOT NULL CHECK (units > 0),
    remaining              BIGINT      NOT NULL CHECK (remaining >= 0 AND remaining <= units),
    source                 TEXT        NOT NULL CHECK (source IN ('polar_benefit', 'manual')),
    polar_benefit_grant_id TEXT,
    rollover               BOOLEAN     NOT NULL DEFAULT false,
    period_id              UUID        REFERENCES billing_period (period_id),
    granted_at             TIMESTAMPTZ NOT NULL,
    expires_at             TIMESTAMPTZ,
    reason                 TEXT,
    actor                  TEXT        NOT NULL
);
CREATE INDEX billing_credit_account_idx ON billing_credit (account_id, meter_key, granted_at, credit_id);
-- One row per Polar grant and cycle; a one-time grant has no period.
CREATE UNIQUE INDEX billing_credit_grant_idx ON billing_credit (polar_benefit_grant_id, period_id) NULLS NOT DISTINCT
    WHERE polar_benefit_grant_id IS NOT NULL;

CREATE TABLE billing_notification (
    notification_id UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL REFERENCES billing_account (account_id),
    period_id       UUID        REFERENCES billing_period (period_id),
    meter_key       TEXT,
    kind            TEXT        NOT NULL,
    details         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,
    CONSTRAINT billing_notification_once UNIQUE NULLS NOT DISTINCT (account_id, period_id, meter_key, kind)
);

CREATE TABLE billing_audit_log (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id     UUID,
    action         TEXT        NOT NULL,
    actor          TEXT        NOT NULL,
    before         JSONB,
    after          JSONB,
    correlation_id TEXT        NOT NULL,
    at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX billing_audit_account_idx ON billing_audit_log (account_id, id);

CREATE TABLE billing_schema_version (
    version    INT         PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed: the initial meters and the free plan (5.8).
INSERT INTO billing_meter (meter_key, name, unit, aggregation, polar_event_name) VALUES
    ('api_calls', 'API calls', 'call',  'count', 'api_call'),
    ('tokens_in', 'Input tokens', 'token', 'sum', 'tokens_in'),
    ('tokens_out', 'Output tokens', 'token', 'sum', 'tokens_out');

INSERT INTO billing_plan (plan_key, version, name, is_recurring, status, source) VALUES ('free', 1, 'Free', false, 'active', 'manual');
INSERT INTO billing_plan_current VALUES ('free', 1);
INSERT INTO billing_plan_meter (plan_key, version, meter_key, included_units) VALUES
    ('free', 1, 'api_calls', 1000), ('free', 1, 'tokens_in', 100000), ('free', 1, 'tokens_out', 100000);
```

The `-- down` section drops the objects in reverse order. `billing_cap` rather than columns on `billing_entitlement`: caps outlive periods. The plan mirror tables are in 0001 rather than 0002 because the free plan, and with it every entitlement row, needs them from BM1; the catalog sync of BM3 only fills them.

### 6.2 Migration 0002, Polar (BM2 and BM3)

```sql
CREATE TABLE billing_polar_event (
    webhook_id   TEXT        PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    sent_at      TIMESTAMPTZ NOT NULL,          -- webhook-timestamp
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    external_id  TEXT,                          -- the customer's external id when the payload has one
    account_id   UUID        REFERENCES billing_account (account_id),
    payload      JSONB       NOT NULL,
    applied_at   TIMESTAMPTZ,
    outcome      TEXT        CHECK (outcome IN ('applied', 'stale', 'unmatched', 'ignored', 'failed')),
    apply_error  TEXT,
    payload_purged_at TIMESTAMPTZ
);
CREATE INDEX billing_polar_event_account_idx ON billing_polar_event (account_id, received_at);
CREATE INDEX billing_polar_event_pending_idx ON billing_polar_event (received_at) WHERE applied_at IS NULL;

CREATE TABLE billing_polar_outbound (
    usage_id             UUID        PRIMARY KEY REFERENCES billing_usage (usage_id),
    account_id           UUID        NOT NULL,
    external_customer_id TEXT        NOT NULL,
    event_name           TEXT        NOT NULL,
    occurred_at          TIMESTAMPTZ NOT NULL,
    metadata             JSONB       NOT NULL,
    state                TEXT        NOT NULL DEFAULT 'pending'
                          CHECK (state IN ('waiting_customer', 'pending', 'sent', 'failed')),
    attempts             INT         NOT NULL DEFAULT 0,
    next_attempt_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error           TEXT,
    queued_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at              TIMESTAMPTZ,
    batch_id             UUID
);
CREATE INDEX billing_polar_outbound_due_idx ON billing_polar_outbound (next_attempt_at, usage_id)
    WHERE state = 'pending';
CREATE INDEX billing_polar_outbound_wait_idx ON billing_polar_outbound (account_id) WHERE state = 'waiting_customer';
CREATE INDEX billing_polar_outbound_sent_idx ON billing_polar_outbound (sent_at) WHERE state = 'sent';

CREATE TABLE billing_subscription (
    subscription_id       TEXT        PRIMARY KEY,
    account_id            UUID        NOT NULL REFERENCES billing_account (account_id),
    polar_customer_id     TEXT        NOT NULL,
    polar_product_id      TEXT        NOT NULL,
    plan_key              TEXT,
    status                TEXT        NOT NULL,
    current_period_start  TIMESTAMPTZ,
    current_period_end    TIMESTAMPTZ,
    cancel_at_period_end  BOOLEAN     NOT NULL DEFAULT false,
    started_at            TIMESTAMPTZ,
    canceled_at           TIMESTAMPTZ,
    ends_at               TIMESTAMPTZ,
    ended_at              TIMESTAMPTZ,
    polar_modified_at     TIMESTAMPTZ NOT NULL,
    applied_webhook_id    TEXT        NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX billing_subscription_account_idx ON billing_subscription (account_id, current_period_start DESC);

CREATE TABLE billing_checkout (
    checkout_id        TEXT        PRIMARY KEY,
    account_id         UUID        REFERENCES billing_account (account_id),
    plan_key           TEXT,
    polar_product_id   TEXT        NOT NULL,
    status             TEXT        NOT NULL,
    url                TEXT,
    expires_at         TIMESTAMPTZ,
    subscription_id    TEXT,
    polar_modified_at  TIMESTAMPTZ NOT NULL,
    created_by         TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE billing_order (
    order_id           TEXT        PRIMARY KEY,
    account_id         UUID        REFERENCES billing_account (account_id),
    subscription_id    TEXT,
    status             TEXT        NOT NULL,
    billing_reason     TEXT,
    total_amount_minor BIGINT      NOT NULL,
    currency           TEXT        NOT NULL,
    period_start       TIMESTAMPTZ,
    period_end         TIMESTAMPTZ,
    lines              JSONB       NOT NULL DEFAULT '[]'::jsonb,
    paid_at            TIMESTAMPTZ,
    polar_modified_at  TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX billing_order_account_idx ON billing_order (account_id, period_start);

CREATE TABLE billing_reconciliation (
    run_id          UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL REFERENCES billing_account (account_id),
    period_id       UUID        REFERENCES billing_period (period_id),
    kind            TEXT        NOT NULL CHECK (kind IN ('live', 'statement')),
    status          TEXT        NOT NULL CHECK (status IN ('match', 'drift', 'error')),
    ledger          JSONB       NOT NULL,   -- per meter: quantity, sent, pending
    polar           JSONB       NOT NULL,   -- per meter: consumed_units, credited_units, balance; order total when present
    drift           JSONB       NOT NULL DEFAULT '[]'::jsonb,
    error           TEXT,
    ran_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX billing_reconciliation_account_idx ON billing_reconciliation (account_id, ran_at DESC);

CREATE TABLE billing_event_log (
    event_id       UUID        PRIMARY KEY,
    event_type     TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    stream_key     TEXT        NOT NULL,
    seq            BIGINT      NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL,
    correlation_id TEXT,
    causation_id   TEXT,
    payload        JSONB       NOT NULL,
    logged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX billing_event_log_key_idx ON billing_event_log (topic, stream_key, seq);
```

### 6.3 Migration 0003, statements (BM4)

```sql
CREATE TABLE billing_statement (
    statement_id   UUID        PRIMARY KEY,
    account_id     UUID        NOT NULL REFERENCES billing_account (account_id),
    period_id      UUID        NOT NULL UNIQUE REFERENCES billing_period (period_id),
    plan_key       TEXT        NOT NULL,
    price_version  INT         NOT NULL,
    currency       TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('final')),
    total_minor    BIGINT      NOT NULL,
    checksum       BYTEA       NOT NULL,
    built_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The link to Polar's order is a separate row rather than a column, so the
-- statement itself never needs an UPDATE (the trigger below forbids one).
CREATE TABLE billing_statement_order (
    statement_id UUID        PRIMARY KEY REFERENCES billing_statement (statement_id),
    order_id     TEXT        NOT NULL,
    linked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE billing_statement_line (
    statement_id      UUID   NOT NULL REFERENCES billing_statement (statement_id),
    line_no           INT    NOT NULL,
    kind              TEXT   NOT NULL CHECK (kind IN ('base', 'meter')),
    meter_key         TEXT,
    quantity          BIGINT NOT NULL,
    included          BIGINT NOT NULL,
    credits_applied   BIGINT NOT NULL,
    billable          BIGINT NOT NULL,
    unit_amount_pico  BIGINT,
    amount_minor      BIGINT NOT NULL,
    PRIMARY KEY (statement_id, line_no)
);

-- Statements are final: like the ledger, they never change.
CREATE TRIGGER billing_statement_immutable BEFORE UPDATE OR DELETE ON billing_statement
    FOR EACH ROW EXECUTE FUNCTION billing_usage_immutable();
CREATE TRIGGER billing_statement_line_immutable BEFORE UPDATE OR DELETE ON billing_statement_line
    FOR EACH ROW EXECUTE FUNCTION billing_usage_immutable();
```

Reconciliation writes `billing_statement_order` once per statement; a second link for the same statement with a different order is `CodeConflict` and a metric (`billing.reconcile.relink`), never an overwrite.

### 6.4 Framework tables

The billing service uses the framework's `mediator_*` tables through `pg.Migrate` as every application does. Every place that enumerates tables for truncation or seeding (the sweep harness's table list, the chaos controller's reset, the integration tier's database per test) learns the `billing_*` tables through `billing.Tables`, in the way handoff section 6 prescribes.

### 6.5 Retention

| Table | Rule | Owner |
|---|---|---|
| `billing_usage`, `billing_statement*`, `billing_period*`, `billing_credit`, `billing_audit_log`, `billing_event_log` | never deleted | |
| `billing_polar_outbound` | rows in state `sent` older than `BILLING_OUTBOUND_RETENTION` (7 days) are deleted | billing janitor |
| `billing_polar_event.payload` | payloads older than `BILLING_EVENT_PAYLOAD_RETENTION` (90 days) are replaced by `'{}'` and `payload_purged_at` set; the row stays | billing janitor |
| `billing_reconciliation` | rows older than 400 days are deleted | billing janitor |
| `billing_notification` | delivered rows older than 90 days are deleted | billing janitor |

### 6.6 Indexes and locks on the hot path

`RecordUsage` touches, in order: `billing_account` and `billing_meter` by primary key (no lock), `billing_usage` by the dedup unique index, `billing_period` by `billing_period_open_idx` and `billing_period_grace_idx`, `billing_entitlement` by primary key (no lock), `billing_meter_period` by primary key `FOR UPDATE`, then inserts into `billing_usage` and `billing_polar_outbound`. The only lock held is the counter row's, so two enforced writes of different meters or different accounts never wait on each other, and two of the same (account, meter, period) serialize on one row (B4). Deadlocks are impossible in this path because every transaction takes at most one exclusive row lock; the `FOR KEY SHARE` locks its foreign keys take on the account, meter, and period rows conflict only with key updates and deletes, which no command performs. `RecordUsageBatch` sorts its events by `(meter_key, source_event_id)` and locks counters in `meter_key` order, so two batches on one account cannot deadlock either; a batch that includes several periods (a late event beside a current one) locks in `(meter_key, period_id)` order.

### 6.7 Cache tags

| Tag | Bumped by | Read by |
|---|---|---|
| `billing:account:<account_id>` | `OpenAccount`, `SuspendAccount`, `ReinstateAccount`, `CloseAccount`, `SetCaps`, `GrantCredit`, `LinkPolarCustomer`, `RefreshEntitlements` | `GetAccount`, `GetAccountSummary` |
| `billing:sub:<account_id>` | `RefreshEntitlements` (sent after every `SubscriptionChanged`), `LinkPolarCustomer`, `ApplyPolarObject` | `GetSubscription`, `GetAccountSummary` |
| `billing:catalog` | `UpsertCatalog`, `UpsertMeters` | `ListPlans`, `GetPlan`, `ListMeters` |

`GetAccountSummary` carries the per-meter `used` of the open period and is cached for 30 seconds; `RecordUsage` does not bump the account tag. The staleness of `used` in the summary is therefore bounded by its TTL by design; every decision reads the counter directly (`CheckEntitlement`, `RecordUsage`) and is never cached. This is stated on the operation in OpenAPI (`x-staleness: 30s`). A subscription change applied by the mirror consumer reaches `GetSubscription` when `RefreshEntitlements` bumps the tag one consumer hop later, or at the 5-minute TTL; the synchronous path (`ApplyPolarObject`) bumps it in its own transaction, so the caller of `ChangePlan` never reads its own stale subscription.

### 6.8 Hot-path SQL

Appendix E gives the statements of `RecordUsage` and the webhook apply verbatim, in the way spec 6.3 gives the outbox write path.

### 6.9 Migrations

`billing.Migrate(ctx, pool)` applies `billing/migrations` through `pg.Migrator{FS: migrations.FS, VersionTable: "billing_schema_version", LockName: "billing_migrate"}` (F2) after `pg.Migrate` has applied the framework's. Migration 0001 creates `billing_schema_version` and its `-- down` section drops it, exactly as the framework's `0001_init` does for `mediator_schema_version`, which is the convention `pg.Migrator` keeps. Files are `NNNN_name.sql` with a `-- down` section; `TestBillingMigrations_UpDownUp` applies, reverts, and re-applies every migration on an empty database and on one seeded by the previous version, exactly as the framework's test does. Migrations are append-only once merged, and every new migration is named in `TestBillingMigrationsEmbedded` and in the `migrate` subtests of `cmd/billingctl`.

---

## 7. Requests

### 7.1 Conventions

- Every request type pins its persisted name with `Named` under the `billing.` namespace (`billing.RecordUsage`), so a Go rename never changes the idempotency scope, the OpenAPI operation ID, or the audit log.
- Every request has a REST route under `/billing` (Appendix B). The server is built with `httpapi.Config{Prefix: "/billing"}`; the paths in this document are the served paths, so a `Route()` declares them without the prefix (`/accounts`, not `/billing/accounts`). System commands keep the RPC default, which the framework serves under the prefix as `POST /billing/rpc/billing.X`, because nobody calls them over HTTP (4.3).
- Every request implements `Requires()` (4.3) and `Describe()` (summary, tags, documented errors).
- Commands that a person or a service retries carry an idempotency key: self-service and admin commands from the `Idempotency-Key` header; service commands (`RecordUsage`, `RecordUsageBatch`) from the body (`IdempotencyKey()` returns the event or batch ID), because the event ID is the thing that must be idempotent and a header could disagree with it.
- Commands whose handler may hit a serialization failure or a deadlock carry `RetryPolicy{MaxAttempts: 3, BaseDelay: 20ms, MaxDelay: 200ms}`; commands that call Polar carry none (they have no unit of work, and the standard `Retry` rejects that combination) and document that the client retries.
- Amounts are `int64` minor units; quantities are `int64` units; instants are RFC 3339 in UTC; IDs are strings.
- Pagination is keyset: `cursor` (opaque, the last item's ID) and `size` (1 to 200, default 50); responses carry `nextCursor`.
- Errors follow spec 4.9. Domain errors used here: `CodeNotFound` (account, period, statement, plan, meter), `CodePrecondition` with `Details.reason` (`quota_exceeded`, `period_not_open`, `no_active_subscription`, `already_subscribed`, `account_closed`, `meter_retired`, `negative_counter`), `CodeForbidden` with `Details.reason` (`account_suspended`, `not_owner`), `CodeConflict` (`stale_webhook`), `CodeIdempotencyMismatch`, `CodeUnavailable` (Polar unreachable on a synchronous call), and `CodeValidation` with field paths.

### 7.2 Commands

| Name | Route | Class | Result | Traits beyond Requires and Describe |
|---|---|---|---|---|
| `OpenAccount` | `POST /billing/accounts` 201 | self-service, admin | `AccountView` | header key; `Invalidates(account)`; `RetryPolicy` |
| `SuspendAccount`, `ReinstateAccount`, `CloseAccount` | `POST /billing/accounts/{accountId}/suspend`, `/reinstate`, `/close` | admin (close: also owner) | `Void` | header key; `Invalidates(account, sub)` |
| `SetCaps` | `PUT /billing/accounts/{accountId}/caps/{meterKey}` | self-service | `CapsView` | header key; `Invalidates(account)` |
| `RecordUsage` | `POST /billing/usage` | service | `RecordUsageResult` | body key (`eventId`); `RetryPolicy`; `Validate` (attributes, window, count meters) |
| `RecordUsageBatch` | `POST /billing/usage/batch` | service | `RecordUsageBatchResult` | body key (`batchId`); `RetryPolicy`; `Validate` (1 to 1000 events, distinct event IDs) |
| `AdjustUsage` | `POST /billing/accounts/{accountId}/adjustments` | admin | `RecordUsageResult` | body key (`adjustmentId`); `RetryPolicy` |
| `GrantCredit` | `POST /billing/accounts/{accountId}/credits` 201 | admin | `CreditView` | header key; `Invalidates(account)`; publishes `CreditGranted` |
| `StartCheckout` | `POST /billing/accounts/{accountId}/checkout` 201 | self-service | `CheckoutView` | `NoUnitOfWork`; `Timeout(15s)`; `RateLimit(10/min per subject)` |
| `OpenPortal` | `POST /billing/accounts/{accountId}/portal` 201 | self-service | `PortalView` | `NoUnitOfWork`; `Timeout(15s)`; `RateLimit(10/min per subject)` |
| `ChangePlan` | `POST /billing/accounts/{accountId}/subscription/plan` | self-service | `SubscriptionView` | `NoUnitOfWork`; `Timeout(15s)` |
| `CancelSubscription` | `POST /billing/accounts/{accountId}/subscription/cancel` | self-service | `SubscriptionView` | `NoUnitOfWork`; `Timeout(15s)` |
| `UncancelSubscription` | `POST /billing/accounts/{accountId}/subscription/uncancel` | self-service | `SubscriptionView` | `NoUnitOfWork`; `Timeout(15s)` |
| `ApplyPolarEvent` | `POST /billing/rpc/billing.ApplyPolarEvent` (reached only from the webhook mount) | webhook | `ApplyPolarEventResult` | body key (`webhookId`); `Timeout(5s)` |
| `SyncCatalog` | `POST /billing/catalog/sync` | admin | `CatalogSyncResult` | `NoUnitOfWork`; `Timeout(60s)` |
| `UpsertMeters` | `PUT /billing/meters` | admin | `MetersView` | header key; `Invalidates(catalog)` |
| `ClosePeriod` | `POST /billing/periods/{periodId}/close` | admin (`force`), system | `PeriodView` | `RetryPolicy`; `TxOptions{Serializable}` |
| `Reconcile` | `POST /billing/accounts/{accountId}/reconcile` | admin, system | `ReconciliationView` | `NoUnitOfWork`; `Timeout(30s)` |
| `LinkPolarCustomer` | rpc | system | `Void` | `Invalidates(account, sub)` |
| `UpsertCatalog` | rpc | system | `CatalogSyncResult` | `Invalidates(catalog)` |
| `RefreshEntitlements` | rpc | system | `Void` | `Invalidates(account, sub)`; `RetryPolicy` |
| `OpenPeriod` | rpc | system | `PeriodView` | `RetryPolicy`; `TxOptions{Serializable}` |
| `GracePeriod` | rpc | system | `PeriodView` | `RetryPolicy`; `TxOptions{Serializable}`; moves an open period to grace, optionally shortening `ends_at` to a new boundary |
| `BuildStatement` | rpc | system | `StatementView` | `RetryPolicy`; `TxOptions{Serializable}` |
| `RecordCheckout` | rpc | system | `Void` | records a checkout session the account started (7.2.4); idempotent on the checkout ID |
| `ApplyPolarObject` | rpc | system | `Void` | `Invalidates(account, sub)`; applies a subscription object returned by a synchronous Polar call through `mirror.Apply`, with the same `modified_at` guard as a webhook (7.2.4) |
| `RecordReconciliation` | rpc | system | `Void` | |
| `RecordNotification` | rpc | system | `Void` | `TxOptions{Propagation: RequiresNew}`, so the `cap_reached` row of a rejected write commits although the caller's transaction rolls back (7.2.2); idempotent by the unique key of 5.10 |

#### 7.2.1 `OpenAccount`

```go
type OpenAccount struct {
    mediator.Command[AccountView]
    Kind       string `json:"kind" validate:"required,oneof=user org"`
    ExternalID string `json:"externalId" validate:"required,max=128,pattern=^[A-Za-z0-9_.:-]+$"`
    Email      string `json:"email" validate:"required,email,max=254"`
    Name       string `json:"name" validate:"max=256"`
}
```

A non-admin may open only the account of its own principal: `kind = user` with `ExternalID = Subject`, or `kind = org` with `ExternalID = Tenant`. Handler: insert the account (a duplicate external ID is `CodeConflict` unless the existing account is the caller's own, which returns it unchanged, so an application that calls `OpenAccount` on every login is safe even without the header key), open the calendar period containing now (`OpenPeriod` nested), create the free plan's entitlement rows, publish `AccountChanged` (audit) and `AccountOpened` (durable, key = account ID).

#### 7.2.2 `RecordUsage`

```go
type RecordUsage struct {
    mediator.Command[RecordUsageResult]
    AccountID  string            `json:"accountId" validate:"required,uuid"`
    MeterKey   string            `json:"meterKey" validate:"required,pattern=^[a-z][a-z0-9_]{0,63}$"`
    Quantity   int64             `json:"quantity" validate:"required,min=1,max=1000000000000"`
    OccurredAt *time.Time        `json:"occurredAt"`                      // default now
    EventID    string            `json:"eventId" validate:"required,min=1,max=200"`
    Attributes map[string]string `json:"attributes" validate:"max=20,dive,max=200"`
    Mode       string            `json:"mode" validate:"oneof=enforce record"` // default enforce
}

type RecordUsageResult struct {
    UsageID      string `json:"usageId"`
    Deduplicated bool   `json:"deduplicated"`
    PeriodID     string `json:"periodId"`
    Late         bool   `json:"late"`
    Used         int64  `json:"used"`      // counter after this event
    Remaining    int64  `json:"remaining"` // -1 when unlimited
    Status       string `json:"status"`    // ok, soft_exceeded, hard_exceeded (record mode only)
}
```

Semantics, in order (the transaction is Appendix E.1):

1. Account must exist (`CodeNotFound`) and be `active` (`CodeForbidden` `account_suspended` in enforce mode; record mode proceeds for `suspended`; `closed` rejects both with `CodePrecondition` `account_closed`).
2. Meter must exist (`CodeNotFound`) and be active (`CodePrecondition` `meter_retired`); a `count` meter requires `quantity = 1` (`CodeValidation` on `/quantity`).
3. `occurredAt` must be inside the window (`CodeValidation` on `/occurredAt`, rule `window`).
4. Dedup by `(accountId, eventId)`: same hash returns `deduplicated: true` with the original `usageId` and the current counters; different hash is `CodeIdempotencyMismatch`.
5. The period is assigned (5.4); `late` is set.
6. The counter row is locked; `entitle.Decide` runs with the entitlement row (or the plan defaults when none exists yet). Enforce mode rejects with `CodePrecondition` `quota_exceeded` and rolls back, after a nested `Send(RecordNotification{cap_reached})` whose `RequiresNew` transaction commits on its own (5.10). In every mode the counter is updated, the ledger row written with `used_after` and `cap_at_write`, and, when the meter has a Polar event name, the outbound row queued (state `pending` when the account is linked, `waiting_customer` otherwise).
7. Threshold crossings publish `UsageThresholdCrossed` (durable, key = account ID, one event per crossing). Nothing else is published per usage event: the ledger row is the record, and per-event outbox rows would double the write volume for no consumer (8.2).
8. The result is returned; the framework stores it for the idempotency replay.

`RecordUsageBatch` takes 1 to 1000 events for one account (`batchId` is the idempotency key; every `eventId` distinct within the batch) and applies the same steps in one transaction, all or nothing: one rejected enforce-mode event fails the batch with `CodePrecondition` and `Details.index`. Duplicates inside the batch against already recorded events are not failures; each result carries its own `deduplicated`. The batch result is the list of per-event results in request order.

#### 7.2.3 `AdjustUsage`

An adjustment is a ledger row with `source = adjustment`, any non-zero quantity, a `reason`, and optionally the `usageId` it corrects (`adjusts`). It goes through the same period assignment and counter update (a negative quantity reduces the counter; the counter may not go below zero: `CodePrecondition` `negative_counter`), is never enforced, is never ingested into Polar (17.2, question 4), and writes the audit log.

#### 7.2.4 Commands that call Polar synchronously

`StartCheckout`, `OpenPortal`, `ChangePlan`, `CancelSubscription`, `UncancelSubscription`, `SyncCatalog`, and `Reconcile` are `NoUnitOfWork` commands whose handlers call Polar (10.6) and then `Send` a system command that records the result in its own unit of work. A crash between the call and the record leaves nothing to clean up: every one of these Polar operations is reported back by a webhook (`checkout.created`, `subscription.updated`, and so on), so the mirror learns the outcome regardless. The result of each is the mirror's view after the record, or the Polar object when the mirror has not seen it yet (checkout).

- `StartCheckout{AccountID, PlanKey, SuccessURL, EmbedOrigin?}`: `CodePrecondition` `already_subscribed` when the account has an active subscription (change plans instead), `CodeNotFound` for an unknown plan or a plan with no product. Creates the session with `external_customer_id`, `customer_email`, `success_url`, and `metadata{account_id, plan_key}` and records it (`RecordCheckout`, system). Returns `CheckoutView{CheckoutID, URL, ExpiresAt}`.
- `OpenPortal{AccountID}`: creates a customer session for the account's external ID and returns `PortalView{URL}`. Nothing is recorded.
- `ChangePlan{AccountID, PlanKey, Proration: prorate|invoice|next_period}`: `PATCH /v1/subscriptions/{id}` with `product_id` and `proration_behavior`; `CodePrecondition` `no_active_subscription` without one. The response updates the mirror through `ApplyPolarObject` (system) with the returned subscription, so the caller sees the new plan without waiting for the webhook; the webhook that follows is a no-op by `modified_at`.
- `CancelSubscription{AccountID, AtPeriodEnd bool, Reason?}`: `cancel_at_period_end: true` or `revoke: true`. `UncancelSubscription{AccountID}`: `cancel_at_period_end: false` (Polar's `uncanceled`). Pausing and resuming (Polar's `pause_at_period_end` and `resume`) are not exposed in v1; the mirror applies `subscription.paused` and `subscription.resumed` when Polar sends them.
- `SyncCatalog{}`: pages `GET /v1/products` for the organization and sends `UpsertCatalog` with the result (10.7).
- `Reconcile{AccountID, PeriodID?}`: fetches the customer state and, for a closed period, the order, then sends `RecordReconciliation` (10.8).

#### 7.2.5 `ApplyPolarEvent`

```go
type ApplyPolarEvent struct {
    mediator.Command[ApplyPolarEventResult]
    WebhookID string          `json:"webhookId" validate:"required,max=128"`
    SentAt    time.Time       `json:"sentAt"`
    Type      string          `json:"type" validate:"required,max=64,pattern=^[a-z_]+\\.[a-z_]+$"` // \\. : a struct tag is unquoted like a Go string
    Data      jsontext.Value  `json:"data" validate:"required"`
}
type ApplyPolarEventResult struct {
    Duplicate  bool   `json:"duplicate"`
    ExternalID string `json:"externalId"`
    Matched    bool   `json:"matched"`
}
```

The handler is the whole synchronous work of a webhook, and it is small so the edge answers Polar within its timeout (10.4): insert the row into `billing_polar_event` (`ON CONFLICT (webhook_id) DO NOTHING`; a conflict is `duplicate: true` and nothing else happens), extract the external ID (5.9), resolve the account, and publish `PolarEventReceived{WebhookID, Type, ExternalID, AccountID}` (durable) with stream key = the account ID when matched, the external ID when unmatched, or `catalog` for product, benefit, and organization events. The consumer does the rest (8.3). The event's `Data` is not copied into the outbox; the consumer reads it from the table.

The command is idempotent on `webhookId` twice over: the framework's idempotency behavior replays within 24 hours (Polar retries within that window), and the table's primary key catches anything later.

#### 7.2.6 Period and statement commands

- `OpenPeriod{AccountID, Kind, StartsAt, EndsAt, SubscriptionID?, PlanKey, PriceVersion}` (system): inserts the period, creates the entitlement rows from the plan, publishes `PeriodOpened`. Serializable, so two concurrent openers for one account conflict and one retries to find the other's work done.
- `ClosePeriod{PeriodID, Force bool}`: the period must be in grace (or open when forced, in which case it is graced first with `ends_at = now`); freezes `billing_period_allowance` from the entitlement rows, sets `closed`, publishes `PeriodClosed`. `CodePrecondition` `period_not_open` when already closed; idempotent under the framework key for the admin path.
- `BuildStatement{PeriodID}` (system): the period must be closed; loads its rows, allowance snapshot, and credits, calls `rating.Build`, inserts the statement and its lines, reduces the credits' `remaining` FIFO, publishes `StatementFinalized`. A second call for a period that has a statement returns the existing one (`period_id` is unique) after verifying its checksum against a rebuild; a mismatch is `CodeInternal` and a metric (`billing.statement.mismatch`), because it would mean the ledger changed after close, which the trigger makes impossible.

### 7.3 Queries

| Name | Route | Class | Result | Cache |
|---|---|---|---|---|
| `GetAccount` | `GET /billing/accounts/{accountId}` | self-service | `AccountView` | `account`, 5 min |
| `ResolveAccount` | `GET /billing/accounts/by-external/{kind}/{externalId}` | self-service (own only), admin | `AccountView` | no |
| `GetAccountSummary` | `GET /billing/accounts/{accountId}/summary` | self-service | `AccountSummary` (plan, subscription status, open period, per meter used, allowance, remaining, status) | `account`, `sub`, 30 s |
| `CheckEntitlement` | `GET /billing/accounts/{accountId}/entitlements/{meterKey}?quantity=` | self-service, service | `EntitlementView` (allowed, used, allowance, cap, remaining, status, advisory) | never; `RateLimit(600/min per subject, burst 100)` |
| `GetUsage` | `GET /billing/accounts/{accountId}/usage?periodId=&meterKey=` | self-service | `UsageView` (per meter: quantity, events, per-day series) | no |
| `ListUsageEvents` | `GET /billing/accounts/{accountId}/usage/events?meterKey=&from=&to=&cursor=&size=` | self-service | `Page[UsageEvent]` | no |
| `GetSubscription` | `GET /billing/accounts/{accountId}/subscription` | self-service | `SubscriptionView` | `sub`, 5 min |
| `ListPlans`, `GetPlan` | `GET /billing/plans`, `GET /billing/plans/{planKey}` | authenticated | `[]PlanView`, `PlanView` | `catalog`, 10 min |
| `ListMeters` | `GET /billing/meters` | authenticated | `[]MeterView` | `catalog`, 10 min |
| `ListPeriods` | `GET /billing/accounts/{accountId}/periods?cursor=&size=` | self-service | `Page[PeriodView]` | no |
| `GetStatement`, `ListStatements` | `GET /billing/statements/{statementId}`, `GET /billing/accounts/{accountId}/statements` | self-service | `StatementView`, `Page[StatementView]` | no (immutable; ETag from checksum) |
| `ListCredits` | `GET /billing/accounts/{accountId}/credits` | self-service | `[]CreditView` | no |
| `ListNotifications` | `GET /billing/accounts/{accountId}/notifications?undeliveredOnly=` | self-service | `Page[NotificationView]` | no |
| `GetPolarEvent`, `ListPolarEvents` | `GET /billing/polar/events/{webhookId}`, `GET /billing/polar/events?accountId=&outcome=` | admin | raw event rows | no |
| `ListReconciliations` | `GET /billing/accounts/{accountId}/reconciliations` | admin | `Page[ReconciliationView]` | no |
| `GetIngestStatus` | `GET /billing/polar/ingest` | admin | backlog, oldest pending age, failed count, waiting-customer count, last batch | no |

Queries run in the framework's default read-only `REPEATABLE READ` transaction; `GetUsage` over a period is therefore one snapshot, so its per-meter totals equal the sum of its series.

### 7.4 Streams

`WatchUsage{AccountID, MeterKey?, LastEventID (header Last-Event-ID)}` at `GET /billing/accounts/{accountId}/usage/stream` tails the ledger of an account as Server-Sent Events, one `UsageEvent` per row in `usage_id` order, resumable from `Last-Event-ID` (a usage ID). It is `NoUnitOfWork` with a one-hour timeout and polls through the pool every 500 ms, like `WatchOrder` in the example service. It never ends on its own; the client disconnects.

---

## 8. Events and consumers

### 8.1 In-process notifications

| Event | Published by | Handler | Effect |
|---|---|---|---|
| `AccountChanged{Before, After}` | account commands, `LinkPolarCustomer` | audit | `billing_audit_log` row in the command's transaction |
| `CapsChanged`, `CreditChanged`, `MetersChanged`, `CatalogChanged` | `SetCaps`, `GrantCredit`, `UpsertMeters`, `UpsertCatalog` | audit | same |

A failing audit handler fails the command (spec 4.4), which is the point: a change without its audit row does not happen.

### 8.2 Durable events

All are `Durable` with a pinned `Name` and `Topic`; the stream key is the account ID unless stated.

| Event | Topic | Published by | Payload |
|---|---|---|---|
| `AccountOpened` | `billing.accounts` | `OpenAccount` | account ID, kind, external ID, email, name |
| `UsageThresholdCrossed` | `billing.usage` | `RecordUsage`, `RecordUsageBatch` | account, meter, period, threshold kind and percent, used, allowance or cap |
| `PolarEventReceived` | `billing.polar` | `ApplyPolarEvent` | webhook ID, type, external ID, account ID (key: account ID, else external ID, else `catalog`) |
| `SubscriptionChanged` | `billing.subscriptions` | mirror consumer, `ApplyPolarObject` | account, subscription ID, status, plan key, product, period start and end, cancel-at-period-end, cause (`webhook:<type>` or `api`) |
| `CreditGranted` | `billing.credits` | `GrantCredit`, mirror consumer | account, credit ID, meter, units, source, period ID |
| `PeriodOpened`, `PeriodClosed` | `billing.periods` | `OpenPeriod`, `ClosePeriod` | account, period ID, kind, bounds, plan key, price version |
| `StatementFinalized` | `billing.statements` | `BuildStatement` | account, period ID, statement ID, total, checksum |
| `CatalogSynced` | `billing.catalog` (key `catalog`) | `UpsertCatalog`, `UpsertMeters` | changed plan keys and versions, changed meters |

Why there is no per-usage durable event: every derived state of usage (the counter, the outbound queue, the thresholds) is written in the recording transaction, where it is cheapest and atomic; a consumer would learn nothing the ledger does not already hold. The threshold event exists because notifications and audit need a signal, and crossings are rare.

### 8.3 Durable consumers

| Group | Events | Effect (in the consumer's unit of work, inbox-deduplicated) | Options |
|---|---|---|---|
| `billing_sync` | `AccountOpened` | Create the Polar customer (10.5) or find it by external ID, then nested `Send(LinkPolarCustomer)`. The Polar call happens inside the consumer transaction; it is idempotent by `external_id`, bounded by the handler timeout, and defended by the fencing token being recorded on the account row (`sync_fence`) so a stale owner's late link cannot overwrite a newer one | `HandlerTimeout(20s)`, `MaxAttempts(20)` |
| `billing_mirror` | `PolarEventReceived` | Load the stored payload, `mirror.Apply`, write the mirror tables, move periods (5.4), publish `SubscriptionChanged` and `CreditGranted` as the actions say, mark the event `applied`, `stale`, `unmatched`, or `ignored` | `HandlerTimeout(10s)`, `MaxAttempts(10)`, `StrictOrder(true)`: an event that cannot be applied halts the account's partition rather than being skipped, because a skipped subscription event leaves the mirror wrong for everyone behind it; the operator resolves with `billingctl webhooks` (12.3) |
| `billing_entitlements` | `SubscriptionChanged`, `CreditGranted`, `PeriodOpened` | Nested `Send(RefreshEntitlements)`, which recomputes the rows and bumps the cache tags | `HandlerTimeout(10s)` |
| `billing_notify` | `UsageThresholdCrossed`, `SubscriptionChanged` (past_due, unpaid, canceled, revoked) | Nested `Send(RecordNotification)`; the unique key makes a redelivery a no-op even without the inbox | `HandlerTimeout(5s)` |
| `billing_statements` | `PeriodClosed` | Nested `Send(BuildStatement)` | `HandlerTimeout(60s)`, `MaxAttempts(20)` |
| `billing_reconcile` | `StatementFinalized` | Fetch the customer state and the period's order from Polar (read-only calls inside the consumer transaction), nested `Send(RecordReconciliation)` | `HandlerTimeout(30s)`, `MaxAttempts(20)` |
| `billing_audit` | every billing durable event | One `billing_event_log` row | `HandlerTimeout(5s)` |

Every consumer handler takes its transaction from `pg.TxFrom(ctx)`, sends nested commands with `billing.SystemContext(ctx)`, and reads `mediator.EnvelopeFrom(ctx)` for the event ID and sequence. Per-account ordering of `PolarEventReceived` is what lets the mirror apply subscription events in the order Polar sent them; the `modified_at` guard covers the case where Polar itself delivered out of order.

### 8.4 Partitions and groups

The billing service runs `BILLING_PARTITIONS` (default 4) partitions per topic. There are seven groups and eight topics; a group holds leases only on the topics it consumes, so `billing_audit` holds up to 32 at P = 4 and the others between 4 and 12, all shared by the nodes that run consumers. `billing.Groups` and `billing.Topics()` list them for the relay's `KnownGroups`, the janitor, readiness, and the test harnesses.

---

## 9. Components

### 9.1 `ingest.PolarIngestor`

Moves outbound rows to Polar. One goroutine per node; rows are claimed with `SKIP LOCKED`, so several nodes share the work without a lease. Loop:

```sql
BEGIN;
SELECT usage_id, external_customer_id, event_name, occurred_at, metadata, attempts
FROM billing_polar_outbound
WHERE state = 'pending' AND next_attempt_at <= now()
ORDER BY next_attempt_at, usage_id
LIMIT $1                              -- INGEST_BATCH, default 500
FOR UPDATE SKIP LOCKED;
-- POST /v1/events/ingest with one event per row, external_id = usage_id (10.2)
UPDATE billing_polar_outbound SET state = 'sent', sent_at = now(), batch_id = $2 WHERE usage_id = ANY($3);
COMMIT;
```

The transaction stays open during the POST (bounded by `INGEST_TIMEOUT`, default 10 seconds, and `idle_in_transaction_session_timeout`), which is what keeps two nodes from sending the same rows: a row is locked until its batch is marked or rolled back. Wake-up: a `LISTEN billing_outbound` notification sent by `RecordUsage`'s transaction (through `Tx.Notify`, delivered at commit, deduplicated per transaction), plus a poll every `INGEST_INTERVAL` (default 1 second).

Failure analysis:

| Failure | Effect |
|---|---|
| POST fails before Polar accepted it (connection refused, 5xx, 429) | rollback; every row of the batch is retried after a backoff `min(2^attempts * 1s, 10m)` with jitter, or after the `Retry-After` of a 429; `attempts` is bumped in a separate short transaction so the backoff survives the rollback |
| Polar accepted (2xx) and the mark failed, or the node crashed between the two | the rows are still `pending`; the next batch re-sends them; Polar's `external_id` dedup counts them once and reports them in `duplicates` (10.2); `billing.ingest.duplicates` counts what Polar reported |
| 4xx other than 429 for the batch | the batch is bisected: halves are re-sent until the failing rows are isolated (at most `log2(batch)` extra calls); a row that fails alone is marked `failed` with the response body, counted by `billing.ingest.failed`, and left for `billingctl ingest retry` after the cause is fixed; the rest are sent |
| A row's account is not linked | the row waits in `waiting_customer`; `LinkPolarCustomer` flips the account's waiting rows to `pending` in its own transaction |
| Polar down for a long time | the backlog grows in Postgres; `billing.ingest.backlog` and `billing.ingest.oldest_age` alarm; readiness fails above `INGEST_READY_MAX_AGE` (default 1 hour) so a node that is not draining stops taking traffic while the rest keep recording; nothing is lost (B5) |

The ingestor never reads the ledger: everything it needs was copied into the outbound row at recording time, so a meter renamed later does not change what is sent.

### 9.2 `periods.PeriodCloser`

Runs every `PERIODS_INTERVAL` (default 1 minute) on one node at a time under `pg_try_advisory_lock(hashtext('billing_periods'))`:

1. Every open calendar period with `ends_at <= now`: send `GracePeriod` (system; sets `grace`, `grace_at = now`) and `OpenPeriod` for the next calendar month.
2. Every open subscription period with `ends_at + BILLING_BOUNDARY_SLACK <= now` whose mirror has not moved: the boundary event is late (a missed webhook); grace it and open the next period from the mirror's cycle (`current_period_end` plus the recurring interval) so recording never stalls. When the webhook arrives later, the mirror consumer finds the period already open with the same bounds and does nothing, or adjusts `ends_at` if Polar's bounds differ (5.4).
3. Every grace period with `ends_at + BILLING_LATE_GRACE <= now`: send `ClosePeriod`.

Each step is one command per period; a failure is logged and retried on the next tick. The closer is idempotent by construction: the partial unique indexes make a second opener fail with a unique violation that the handler maps to "already done".

### 9.3 `reconcile.Reconciler`

Runs every `RECONCILE_INTERVAL` (default 24 hours) under an advisory lock and sends `Reconcile{AccountID}` for every account with an open period and a linked Polar customer, at `RECONCILE_RATE` per second (default 5, or 300 per minute, under Polar's 500 requests per minute per organization in production; the sandbox allows 100, so the conformance run uses 1). Statement reconciliations happen through the consumer (8.3); this component covers live drift.

### 9.4 `janitor.Billing`

Runs every `BILLING_JANITOR_INTERVAL` (default 10 minutes) under an advisory lock and applies the retention rules of 6.5 in batches of 5000 rows with short transactions.

---

## 10. Polar integration

Polar facts this section relies on were checked against Polar's documentation on 2026-09-27 and are pinned by sandbox fixtures at BM2 (14.5): production API `https://api.polar.sh`, sandbox `https://sandbox-api.polar.sh` with its own access tokens and the test card `4242 4242 4242 4242`; event ingestion at `POST /v1/events/ingest` with per-event `external_id` deduplication and a response of `{inserted, duplicates}`; meters as filters plus an aggregation over event metadata; metered prices and `meter_credit` benefits on products; the customer state endpoint with `active_subscriptions`, `active_meters[].{consumed_units, credited_units, balance}`, and `granted_benefits`; webhooks per the Standard Webhooks specification for secrets created on or after 2026-09-08 (headers `webhook-id`, `webhook-timestamp`, `webhook-signature`), retried up to ten times with exponential backoff, timed out after 10 seconds with 2 seconds recommended, and an endpoint disabled after ten consecutive failures; the API is versioned by the `Polar-Version` request header (`YYYY-MM`; without it Polar uses the current version, which changes at each quarterly release), and the version also selects the shape of webhook payloads; Polar attributes an ingested event to the billing period in which it received the event, not to the event's `timestamp`; rate limits are 500 requests per minute per organization in production and 100 in the sandbox, answered with 429 and `Retry-After`.

### 10.1 The client (`billing/polar`)

```go
type Client struct { ... }
func New(cfg Config) *Client      // Config{BaseURL, AccessToken, OrganizationID, APIVersion, HTTPClient, Clock, Logger}

func (c *Client) Ingest(ctx, events []Event) (IngestResult, error)                       // POST /v1/events/ingest      fault point polar.ingest (+After)
func (c *Client) CreateCustomer(ctx, in CustomerCreate) (Customer, error)               // POST /v1/customers/         fault point polar.customer.create (+After)
func (c *Client) CustomerByExternalID(ctx, id string) (Customer, error)                 // GET  /v1/customers/external/{id}   polar.customer.get
func (c *Client) CustomerState(ctx, externalID string) (CustomerState, error)           // GET  /v1/customers/external/{id}/state   polar.customer.state
func (c *Client) CreateCheckout(ctx, in CheckoutCreate) (Checkout, error)               // POST /v1/checkouts/         polar.checkout.create (+After)
func (c *Client) CreateCustomerSession(ctx, externalID string) (CustomerSession, error) // POST /v1/customer-sessions/ polar.customersession.create
func (c *Client) UpdateSubscription(ctx, id string, in SubscriptionUpdate) (Subscription, error) // PATCH /v1/subscriptions/{id}   polar.subscription.update (+After)
func (c *Client) ListProducts(ctx, page int) (ProductPage, error)                       // GET  /v1/products?organization_id=&page=   polar.products.list
func (c *Client) GetOrder(ctx, id string) (Order, error)                                // GET  /v1/orders/{id}        polar.order.get
```

Rules:

- Every request carries `Polar-Version: <APIVersion>` (`POLAR_API_VERSION`, default `2026-04`); the fixtures of 10.9 are recorded under that version, and a version change is a deliberate fixture re-record.
- Every call passes through `testkit.Fault` with the point named above before the request; the four that create or change state also call `testkit.FaultAfter` after a 2xx, so the ambiguous kind models "Polar did it and the answer was lost".
- Every call is bounded by the caller's context and by `POLAR_TIMEOUT` (default 10 seconds); the client never retries by itself. Retrying is the caller's decision: the ingestor and the consumers retry with their own backoff; the synchronous commands do not.
- Errors are typed: `*polar.Error{Status, Code, Detail, RetryAfter}`. 429 and 5xx are transient (`mediator.IsTransient` through the `Transient()` method); 4xx are not. `polar.IsConflict(err)` recognizes the 422 that a duplicate `external_id` or email produces (the detail text and the validation error's `loc`, pinned by fixture).
- The wire types are exact structs with `json:"..."` tags and `RejectUnknownMembers` off: Polar adds fields, and a new field must not break the mirror. Fields this system relies on are listed in Appendix C; everything else is carried opaquely in the stored payload.
- The metered `unit_amount` is decoded as a string and parsed by `polar.ParseAmount` to an integer in 10^-12 of a cent (5.7); `cap_amount` and `price_amount` are integers in cents.
- The client refuses to be called inside a request unit of work: `pg.StoreTxFrom(ctx)` present without a consumer envelope is a programming error and returns `polar.ErrInsideRequestTx` (B11). Consumer transactions are allowed because that is the framework's model for effects outside Postgres (spec 6.5).
- Metrics: `billing.polar.request.duration` histogram with `endpoint` and `status`.

### 10.2 Ingest mapping

One outbound row becomes one event:

| Polar field | Value |
|---|---|
| `name` | the meter's `polar_event_name` at recording time |
| `external_customer_id` | the account's external ID |
| `external_id` | the usage ID (a UUIDv7 string), which is what makes re-sends harmless |
| `timestamp` | `occurred_at` in RFC 3339 UTC |
| `external_member_id` | `attributes.member` when present |
| `metadata.quantity` | the quantity (integer); the Polar meter of a `sum` meter aggregates `sum(quantity)`, of a `count` meter `count` |
| `metadata.meter` | the meter key |
| `metadata.usage_id`, `metadata.period_id`, `metadata.account_id`, `metadata.mode` | provenance |
| `metadata.<attribute>` | every other attribute, key and value truncated to Polar's limits (40 and 500), at most 50 keys in total including the ones above; the truncation is recorded in the outbound row's `metadata` so what was sent is what is stored |

Adjustments are not ingested (17.2, question 4). The Polar meter for a billing meter must therefore filter on `name == polar_event_name` and aggregate `metadata.quantity`; `billingctl catalog check` verifies that every active meter with an event name maps to a Polar meter with that filter and aggregation and reports the ones that do not.

### 10.3 Ingest guarantees

Polar's `external_id` deduplication is the second half of at-least-once. The ingestor may send a row twice (9.1); Polar counts it once and says so. `IngestResult.Inserted + Duplicates` equals the batch size on every 2xx; the ingestor treats anything else as a protocol violation, marks the batch `failed`, and alarms (`billing.ingest.protocol`), because the only alternatives are to guess which rows landed or to re-send everything, and the fake and the sandbox conformance both pin that the equation holds.

Whether one bad event fails the whole batch or only itself is question 5 of 17.2; until the sandbox answers it, the bisection of 9.1 handles both.

### 10.4 The webhook edge (`billing/hook`)

`hook.Handler{Secret, LegacySecret, Tolerance, MaxBody, Mediator, Clock}` is mounted at `/billing/webhooks/polar` (F1). On each request:

1. Method must be `POST`, else 405. Body is read up to `MaxBody` (default 1 MiB), else 413. Missing headers are 400.
2. The signature is verified per Appendix F against every `v1,` signature in the header (Polar may send several during a secret rotation) with a constant-time compare; `webhook-timestamp` must be within `Tolerance` (default 5 minutes) of the clock. Failure is 401 with an empty body and a warning log with the `webhook-id`; nothing is stored (B10).
3. The body is decoded leniently (unknown members allowed) into `{type, data}`; a body that is not a JSON object with a string `type` and an object `data` is 400.
4. `mediator.Send(ctx, ApplyPolarEvent{...})` with the webhook principal, a correlation ID equal to the `webhook-id`, and the mediator's own timeout (5 seconds) so the edge always answers before Polar's 10.
5. Success and duplicate are 200 with `{"ok": true, "duplicate": <bool>}`. Any error is 500 with a problem body, so Polar retries; `CodeUnavailable` and `CodeTimeout` are 503 with `Retry-After: 5`.

Polar disables an endpoint after ten consecutive failures, so the edge is deliberately hard to fail: verification and one insert plus one outbox row. The consumer does the work later, and a consumer failure never reaches Polar.

### 10.5 Customer lifecycle

`billing_sync` creates the customer from `AccountOpened` with `{email, name, external_id, metadata{account_id, kind}}`. On `polar.IsConflict`, it fetches by external ID; if that customer's `external_id` is the account's, it links it; if the conflict is on email with a different external ID, the account is marked `sync_state = failed` with the reason and `billing.sync.failed` counts it, for an operator to resolve (the email belongs to another Polar customer, which this system cannot merge). `LinkPolarCustomer{AccountID, PolarCustomerID, Fence}` records the ID, flips the account's `waiting_customer` rows to `pending`, and bumps the cache tags; it is a no-op when the same ID is already linked and `CodeConflict` when a different one is, which is the only way a stale consumer could diverge and is what the fence in the row prevents.

`customer.created` and `customer.updated` webhooks also carry `external_id`; the mirror consumer links from them too when the account has no customer yet, so a customer created by the Better Auth plugin, by a checkout, or by hand in the Polar dashboard is linked as soon as its first webhook arrives.

### 10.6 Checkout and subscription commands

`StartCheckout` sends `{products: [product_id], external_customer_id, customer_email, success_url, metadata: {account_id, plan_key}, customer_metadata: {account_id}}`. The returned session is recorded (`RecordCheckout`) so an operator can correlate; `checkout.updated` with `status = succeeded` and the subscription events that follow do the rest through the mirror. A checkout that expires (`checkout.expired`) is marked and forgotten.

`ChangePlan`, `CancelSubscription`, and `UncancelSubscription` use `PATCH /v1/subscriptions/{id}` (10.1) and apply the returned subscription through `ApplyPolarObject`, so the mirror is right before the webhook lands; the webhook is then stale by `modified_at` or identical. A Polar 4xx is returned to the caller as `CodePrecondition` with Polar's detail (for example a downgrade that Polar refuses); a 5xx or timeout is `CodeUnavailable`, and the caller retries; because none of these commands has a unit of work and each is a single Polar call, a retry after an ambiguous outcome is safe: `PATCH` with the same body is idempotent on Polar's side, and the mirror's guard absorbs the duplicate.

### 10.7 Catalog sync

`SyncCatalog` pages `GET /v1/products` for the organization (archived products included, so plans whose product was archived move to `archived`), then `UpsertCatalog` computes, per product with `metadata.plan_key`, the plan row of 5.8 and compares it with the current version; a difference inserts a new version and moves `billing_plan_current`; meters are matched by `polar_meter_id`, and a metered price whose meter is unknown here is recorded in the result as `unmapped` and skipped (the plan gets `overage = block` for that meter until the meter is created). `product.created` and `product.updated` webhooks trigger the same computation for one product through the mirror consumer. The version bump does not by itself change any open period's entitlement (the period keeps its `price_version` until it closes; new periods take the current version); an operator who wants a price change to apply mid-period runs `billingctl entitlements refresh --account`.

### 10.8 Reconciliation

Two kinds, both recorded in `billing_reconciliation` with the ledger side, the Polar side, and the differences:

- **Live** (`kind = live`, from the reconciler or `Reconcile` on an open period): per meter, the ledger's quantity of API rows in the account's open subscription period, the quantity of rows already `sent`, and the quantity still `pending` or `waiting_customer`, against the customer state's `active_meters[].consumed_units` for the meter. Drift is `consumed_units != sent` (with the pending quantity reported beside it, since it explains a lower Polar number). Live drift is informational: it becomes an alarm only when it persists across two runs with an empty pending queue.
- **Statement** (`kind = statement`, from the `billing_reconcile` consumer after `StatementFinalized`): the statement's per-meter billable units and amounts against the Polar order of the period (found through `billing_order` by `subscription_id` and period, or fetched by ID when the order webhook arrived first). Drift is a billable quantity that differs, or an amount that differs by more than the rounding tolerance of 5.7. Polar attributes an event to the billing period in which Polar received it, not to its `timestamp` (10), so rows of the period that were still in the outbound queue at `ends_at` appear on Polar's next order; the reconciliation row records their quantity per meter (`sent_late`) beside the drift so that boundary lag is told apart from loss. The order is linked to the statement (`billing_statement_order`).

A drift row sets `billing.reconcile.drift_units` and `billing.reconcile.drift_amount` and logs at warn. Nothing is corrected automatically: the ledger is immutable and Polar's invoice is Polar's; the row tells an operator where to look.

### 10.9 The fake Polar (`billing/polar/polartest`)

`polartest.Server` implements the endpoints of 10.1 in memory with Polar's observable semantics: `external_id` deduplication with `{inserted, duplicates}`, customer uniqueness on `external_id` and email (422 with Polar's shape), checkout sessions that succeed when told to, subscriptions that cycle when the fake's clock passes `current_period_end`, products with metered prices and `meter_credit` benefits, meters that aggregate the ingested events, customer state computed from them, and orders generated at cycle from the meters and prices with Polar's rounding as pinned by fixture. It signs and delivers webhooks per Appendix F to the configured endpoints with Polar's retry policy.

It is the same code in three places: in process for unit and integration tests (`polartest.Start(t)`), as `cmd/polarfake` for the compose `billing` profile when `POLAR_ACCESS_TOKEN` is unset, and in the chaos profile behind Toxiproxy (14.6). Its admin API (`/fake/...`) lets a test or the chaos controller script a subscription lifecycle, set a fault mode (`5xx`, `429` with `Retry-After`, `slow`, `drop_after_write` for the ambiguous ingest, `hold_webhooks`, `duplicate_webhooks`, `reorder_webhooks`, `delay_webhooks`), advance its clock, dump its state as JSON for the checkers, and reset.

`polartest.Conformance(t, client, caps)` is the honesty check: the same table of calls and expectations runs against the fake in every test tier and against the sandbox when `POLAR_SANDBOX_TOKEN` is set (`task polar-conformance`, manual). Every sandbox response is recorded under `billing/polar/testdata/sandbox/<Polar-Version>/` as a fixture; the fake's decoders and the client's wire types are tested against those fixtures, so a Polar change shows up as a fixture diff on the next conformance run, and the fake cannot drift from the fixtures without a test failing. This is the `pg/storetest` idea applied to a third party.

---

## 11. HTTP surface and OpenAPI

Every request is one operation under `/billing` (Appendix B) served by `httpapi` with the framework's decoding, binding, problem details, and SSE (spec 8). Beyond the framework's behavior:

- `GET /billing/openapi.json` and `GET /billing/docs` serve the billing document; `api/billing-openapi.json` is the committed copy, regenerated by `task openapi` (which now exports both services' documents) and drift-checked in CI, with `openapi-typescript` and `tsc --noEmit` run on it in the same job as the example service's.
- The system commands appear in the document (they are registered requests) under the tag `system` with the description "not callable over HTTP" and their `billing_system` security scope, so the generated client types them but nobody can call them. `TestSystemRequests_UnreachableOverHTTP` proves it.
- The webhook mount is not an operation and is documented by hand in the document's `info.description` with a link to Appendix F.
- `GetAccountSummary` and the other cached queries carry `x-staleness` with their TTL; `CheckEntitlement` carries `x-staleness: none`.
- `Idempotency-Key` is documented on every command that reads it; the body-keyed commands document the field instead.
- Rate-limited operations (`CheckEntitlement`, `StartCheckout`, `OpenPortal`) document 429 with `Retry-After`.

The document is validated against the OpenAPI 3.1 meta-schema at generation and in `TestBillingOpenAPI_MatchesCommitted`, which also asserts the operation IDs are stable (`billingRecordUsage`, and so on: set through `Describe().OperationID` as the request name in lowerCamelCase with the namespace dot removed, since the derived identifier would keep the dot) because the TypeScript client's function names are made from them.

---

## 12. Observability and operations

### 12.1 Metrics

Under the OTel meter `billing`. Names are stable; attributes are bounded by the registry, the meter table, and small enums.

| Name | Type | Attributes |
|---|---|---|
| `billing.usage.recorded` | counter | `meter`, `mode`, `source` |
| `billing.usage.rejected` | counter | `meter`, `reason` (`quota_exceeded`, `account_suspended`, `account_closed`, `meter_retired`, `window`) |
| `billing.usage.deduplicated` | counter | `meter`, `path` (`idempotency`, `ledger`) |
| `billing.usage.late`, `billing.usage.early` | counter | `meter` |
| `billing.entitlement.decisions` | counter | `meter`, `status` |
| `billing.ingest.backlog` | gauge | `state` (`pending`, `waiting_customer`, `failed`) |
| `billing.ingest.oldest_age` | gauge s | |
| `billing.ingest.batches` | counter | `outcome` (`sent`, `retry`, `bisect`, `failed`, `protocol`) |
| `billing.ingest.sent`, `billing.ingest.duplicates` | counter | |
| `billing.webhook.received` | counter | `type`, `outcome` (`accepted`, `duplicate`, `rejected_signature`, `rejected_timestamp`, `malformed`, `error`) |
| `billing.webhook.pending` | gauge | rows in `billing_polar_event` with `applied_at IS NULL` |
| `billing.webhook.apply_lag` | gauge s | age of the oldest unapplied event |
| `billing.mirror.applied` | counter | `type`, `outcome` (`applied`, `stale`, `unmatched`, `ignored`, `failed`) |
| `billing.subscription.multiple`, `billing.catalog.ignored`, `billing.sync.failed` | counter | |
| `billing.period.transitions` | counter | `kind`, `to` (`grace`, `closed`), `cause` (`time`, `boundary`, `forced`, `late_boundary`) |
| `billing.statement.built` | counter | |
| `billing.statement.mismatch` | counter | |
| `billing.reconcile.runs` | counter | `kind`, `status` |
| `billing.reconcile.drift_units`, `billing.reconcile.drift_amount` | gauge | `meter` |
| `billing.credit.granted` | counter | `source` |
| `billing.polar.request.duration` | histogram s | `endpoint`, `status` |

The framework's `mediator.*` metrics (spec 9.1) cover request durations and outcomes, consumer lag, idempotency outcomes, and cache results for every billing request and group.

### 12.2 Health

`GET /billing/healthz` is the framework's liveness. `GET /billing/readyz` adds to the framework's checks (Postgres, Redis, every component's `Healthy()`, consumer lag under `ReadyMaxLag`): the ingest backlog's oldest pending row is younger than `INGEST_READY_MAX_AGE` (1 hour) and the oldest unapplied webhook is younger than `WEBHOOK_READY_MAX_LAG` (10 minutes). A node whose ingestor or consumers are stuck stops receiving traffic; recording continues on the others and the stuck node keeps draining.

### 12.3 CLI (`billingctl`)

`cmd/billingctl` embeds the framework's `ctl.Main` with the billing registry (so `names`, `openapi export`, `migrate`, `outbox`, `inbox`, `idem`, `consumer lag`, `dlq`, and `lease` work on the billing service) and adds:

| Command | Purpose |
|---|---|
| `billing migrate up`, `billing migrate status` | the billing migrations (6.9); `migrate up` runs the framework's first |
| `accounts show --id | --external <kind> <id>` | the account, its open period, entitlements, subscription, and sync state |
| `accounts link --id --polar-customer` | force a link (writes through `LinkPolarCustomer`) |
| `usage export --account --period [--format csv|jsonl]` | the ledger rows of a period |
| `usage adjust --account --meter --quantity --reason --id` | `AdjustUsage` |
| `entitlements refresh --account` | `RefreshEntitlements` |
| `periods list --account`, `periods close --id [--force]` | inspect and close |
| `statements show --id`, `statements rebuild --period` (dry run: rebuilds in memory and diffs against the stored lines) | verify determinism in production |
| `ingest status`, `ingest retry --failed [--account]`, `ingest drain --once` | the outbound queue |
| `webhooks show --id`, `webhooks list --account [--outcome]`, `webhooks reapply --id` (re-publishes `PolarEventReceived`), `webhooks resume --account` (skips the halting event and resumes the partition, like `dlq skip`) | the inbound side |
| `catalog sync`, `catalog check` | 10.7 and 10.2 |
| `meters apply -f meters.json` | `UpsertMeters` |
| `reconcile --account [--period]`, `reconcile list --account` | 10.8 |
| `polar conformance` | the sandbox conformance run (10.9) |

Every command is a Go function in `billing/ops` used by the tests directly.

### 12.4 Configuration

`cmd/billingd` reads the environment (the framework never does):

| Variable | Default | Meaning |
|---|---|---|
| `PG_URL`, `REDIS_ADDR`, `REDIS_PREFIX`, `NODE_ID`, `HTTP_ADDR` | as the example service; `HTTP_ADDR` `:8081` | infrastructure |
| `BILLING_PARTITIONS` | 4 | P for the billing topics |
| `POLAR_BASE_URL` | `https://api.polar.sh` | the API; `https://sandbox-api.polar.sh` for the sandbox; the fake's URL in compose |
| `POLAR_ACCESS_TOKEN`, `POLAR_ORGANIZATION_ID` | required unless `POLAR_BASE_URL` points at the fake | credentials |
| `POLAR_API_VERSION` | `2026-04` | the `Polar-Version` header on every call (10.1); a change means re-recording the fixtures of 10.9 |
| `POLAR_WEBHOOK_SECRET`, `POLAR_WEBHOOK_LEGACY_SECRET` | required; legacy optional | Appendix F |
| `POLAR_TIMEOUT` | 10s | per call |
| `AUTH_JWKS_URL`, `AUTH_ISSUER`, `AUTH_AUDIENCE`, `AUTH_ORG_CLAIM`, `AUTH_JWKS_TTL`, `AUTH_JWT_HS256_SECRET` | 4.1 | authentication |
| `BILLING_OCCURRED_AT_PAST`, `BILLING_OCCURRED_AT_FUTURE` | 72h, 5m | 5.3 |
| `BILLING_LATE_GRACE`, `BILLING_BOUNDARY_SLACK` | 72h, 6h | 5.4 |
| `INGEST_BATCH`, `INGEST_INTERVAL`, `INGEST_TIMEOUT`, `INGEST_READY_MAX_AGE` | 500, 1s, 10s, 1h | 9.1 |
| `PERIODS_INTERVAL`, `RECONCILE_INTERVAL`, `RECONCILE_RATE`, `BILLING_JANITOR_INTERVAL` | 1m, 24h, 5, 10m | 9.2 to 9.4 |
| `BILLING_OUTBOUND_RETENTION`, `BILLING_EVENT_PAYLOAD_RETENTION` | 7d, 90d | 6.5 |
| `WEBHOOK_TOLERANCE`, `WEBHOOK_READY_MAX_LAG` | 5m, 10m | 10.4, 12.2 |
| `BILLING_DEBUG` | 0 | registers the debug requests (a `Sleep` like the example's, and `AdvanceClock` for tests) |

`billing.Config` validates the combination at start and reports every problem at once, like `Build` does.

### 12.5 Logging

The framework's conventions (spec 9.5) apply. Billing adds one record per: enforce-mode rejection (info, with account, meter, used, cap), outbound batch (debug; warn on retry, error on `failed` and `protocol`), webhook rejection (warn, with `webhook-id` and reason, never the body), mirror outcome other than `applied` (info for `stale` and `ignored`, warn for `unmatched`, error for `failed`), period transition (info), statement built (info, with total and checksum), reconciliation drift (warn, with the drift), and sync failure (error). Request bodies are never logged; `attributes` are tagged `log:"redact"`.

---

## 13. Guarantees

Every row is a claim the billing system makes, the conditions under which it holds, and the test that would catch its violation. Invariants IB1 to IB8 are in 14.4; workload names in 14.6. The framework's guarantees G1 to G17 are inherited and not restated.

| ID | Guarantee | Preconditions | Verified by |
|---|---|---|---|
| B1 | The ledger and the statements are append-only: no `UPDATE` or `DELETE` of `billing_usage`, `billing_statement`, or `billing_statement_line` ever succeeds, from any code path or operator session. | | Integration `TestLedger_Immutable` (update and delete as the application role and as the owner fail with `BL001`); the sweep and chaos invariant checks re-read every row's `request_hash` and `used_after` against a checksum taken when written. |
| B2 | One usage event ID produces at most one ledger row per account, forever: a replay within the idempotency window returns the original response byte for byte; a replay after it returns `deduplicated: true` with the original usage ID; a replay with a different body is rejected. | Response type round-trips through JSON. | Unit `TestRecordUsage_Dedup*` on a real store; integration `TestRecordUsage_ReplayAfterIdempotencyExpiry` (expires the framework row with `idem purge` then replays); sweep `SweepRecordUsage` with two clients on one event ID; chaos `billing-usage-retry`; IB7. |
| B3 | For every (account, meter, period), the counter equals the sum and count of its ledger rows at every commit boundary. | Handlers write through the unit of work. | IB1 after every sweep cell and every chaos run, and periodically during soak; property `PropCounter_EqualsSum` over random interleavings on memstore. |
| B4 | An enforced write never takes a counter past the effective cap in force at that instant; concurrent enforced writes of one (account, meter, period) are serialized, on one node or many. | The cap is constant during the write (a cap change is a separate command and takes effect for later writes). | Unit boundary rows (`used + qty == cap` accepted, `cap + 1` rejected); integration `TestRecordUsage_ConcurrentEnforce` with 64 goroutines against a cap; sweep `SweepRecordUsage`; chaos `billing-usage` with the capped-counter Porcupine model (Appendix G); IB6. |
| B5 | Every API ledger row of a meter with a Polar event name is sent to Polar at least once and counted by Polar exactly once, while Polar is eventually reachable; after all faults heal the outbound queue drains within the recovery bound. | The account gets linked; Polar's `external_id` dedup holds. | Sweep `SweepIngest` with every fault kind at `polar.ingest` including ambiguous and crash; chaos `billing-ingest` with Polar partitions, 5xx, slowness, and node kills; IB2 (the fake's accepted set equals the sent set and every ID was inserted once); liveness LB1. |
| B6 | Every Polar webhook is applied at most once, and the mirror equals the state implied by the newest event per subscription regardless of duplicate or out-of-order delivery. | | Unit `TestHook_*` and property `PropMirror_Commutes` (any permutation with duplicates of a lifecycle yields the same state); sweep `SweepWebhook` with every fault at every step and three deliveries of each event; chaos `billing-subscriptions` with `webhook-storm` and `webhook-late`; IB3. |
| B7 | Every ledger row belongs to exactly one period; an account's periods are contiguous and non-overlapping; exactly one is open and at most one in grace; a row's period contains `occurred_at` unless `late` (or `early` at a boundary within the future window). | | Property `PropPeriods_Partition` over random subscription timelines; sweep `SweepPeriodClose`; chaos `billing-periods` with clock skew; IB4. |
| B8 | A statement is a pure function of its period's ledger rows, allowance snapshot, credits, and price version: rebuilding it yields byte-identical lines and the recorded checksum; a final statement never changes. | | Property `PropRating_Deterministic` (row permutation) and unit rounding tables; integration `TestStatement_RebuildMatches`; `billingctl statements rebuild` in the chaos `billing-periods` checkers; IB5; B1. |
| B9 | After a command that changes an account's limits returns (`SetCaps`, `GrantCredit`, `RefreshEntitlements` after a subscription change), no cached read of `GetAccountSummary` or `GetSubscription` returns the previous limits; `CheckEntitlement` and `RecordUsage` are never cached. | Redis reachable for the bumps; otherwise bounded by the TTL (spec G9). | Integration `TestEntitlement_NoStaleAfterChange`; chaos `billing-subscriptions` staleness checker with degraded windows from the nemesis log; IB8. |
| B10 | The webhook edge stores nothing for a request whose signature or timestamp fails, and answers every verified request within its 5-second budget. | | Unit `TestHook_RejectsForged`, `TestHook_RejectsStale`, fuzz `FuzzWebhookVerify` (no random input verifies against a random secret); integration `TestHook_Latency` (p99 under 50 ms at 200 requests per second on the reference stack); `BenchmarkWebhookVerify`. |
| B11 | No Polar call happens inside a request unit of work; Polar calls inside consumer transactions are idempotent operations only (customer create by external ID, reads). | | Unit `TestPolarClient_RefusesRequestTx`; a static test (`TestNoPolarInRequestHandlers`) that parses `billing/handlers*.go` and fails on a `polar.Client` method call reachable from a command with a unit of work. |
| B12 | Liveness: within `RecoveryBound` after all faults heal, the outbound queue has no pending row older than the bound, no stored webhook is unapplied, no period is past due for grace or close, and no billing consumer has pending entries older than the bound. | Faults healed; nodes running. | Chaos liveness LB1 at the end of every billing run; the sweep's recovery step for every scenario. |
| B13 | An adjustment or a late event never changes a closed period's statement; it lands in the open period and appears on the next statement. | | Unit `TestAssignPeriod_*`; sweep `SweepPeriodClose` (adjustments and late events fired at every step of the close); IB5 and IB4. |
| B14 | Every request declares its requirement; account-scoped self-service requests are answered only for the owner or an admin; system requests are unreachable over HTTP; reserved claims in a token are stripped. | | Unit `TestRequirements_Matrix` (every request times every principal class); `TestSystemRequests_UnreachableOverHTTP`; `TestAuth_StripsReservedClaims`; the framework's `RequireAuthByDefault` at Build. |
| B15 | A rejected enforce-mode write leaves no row anywhere and bumps no counter; a batch is all or nothing. | | Sweep `SweepRecordUsage` (a rejection at every fault point); integration `TestRecordUsageBatch_AllOrNothing`; IB1. |
| B16 | Performance on the reference stack: `RecordUsage` p50 under 3 ms and p99 under 15 ms at 1000 events per second per node; `CheckEntitlement` p99 under 2 ms; `entitle.Decide` and `rating` per row allocate nothing; webhook verification under 5 µs. | Reference stack of spec 15.2 question 5. | `BenchmarkRecordUsage_Tx` (integration tag), `BenchmarkHTTP_RecordUsage`, `BenchmarkEntitleDecide` with `testing.AllocsPerRun`, `BenchmarkRating_1kRows`, `BenchmarkWebhookVerify`; `task bench` gates sec/op at 10 percent. |

### 13.1 Explicit non-guarantees

- Record-mode writes and check-then-record callers may exceed a cap (5.5).
- Polar's invoice may differ from a statement by rounding; reconciliation reports it (5.7, 10.8).
- The mirror is eventually consistent with Polar: between a Polar change and the webhook's application, `GetSubscription` shows the old state. The lag is bounded by the webhook's delivery plus the consumer's lag, both measured (12.1), not by a promise.
- Adjustments are not reflected in Polar's meters (17.2, question 4).
- A `past_due` subscription does not suspend the account (5.9).
- Usage recorded before an account is linked to a Polar customer waits; it is not lost, but Polar's meters lag until the link exists (10.5).
- Polar bills an event in the period in which it received it; a row sent after its period's boundary lands on the next Polar order while the statement here keeps it in the period of `occurred_at` (10.8).

---

## 14. Testing strategy

The program is spec 11's, applied to the billing module: SQLite's exhaustive attitude toward the code (coverage, boundary tables, fuzzing, mutation, every I/O site failing every way) and Jepsen's adversarial attitude toward the system (nodes, faults, a recorded history, checkers). Billing adds a third party to the picture; the fake Polar (10.9) is what makes Polar's failures injectable and Polar's state checkable.

### 14.1 Tiers

| Tier | Where | Needs | Gate |
|---|---|---|---|
| 0 Static | everywhere | nothing | the framework's tools over `./...`, unchanged |
| 1 Unit | `*_test.go` beside code | nothing; memstore and the in-process fake Polar | `-race -shuffle=on -count=2`; coverage gate: 100 percent for `billing/entitle`, `billing/rating`, `billing/mirror`, and `billing/polar` (its HTTP transport branches count under the `integration` tag, as the framework's drivers do); 95 percent for the rest of `billing/...` |
| 2 Property and fuzz | beside code, corpora committed | nothing | 30 s per target on every dispatch, 30 min with `long`; any crash is a regression test |
| 3 Fault sweep | `test/faultsweep/sweep_billing_*` | Postgres and Redis containers, the in-process fake Polar | every billing scenario passes for every point and kind; the completeness gate covers the extended catalogue |
| 4 Integration | `*_integration_test.go` beside code; `test/integration/billing_*` | containers; `billingd` built once | real-service behavior; the Polar conformance suite against the fake (and the sandbox on demand) |
| 5 Chaos | `test/chaos/billing_*` | compose `chaos` profile with `polarfake` | every billing workload's checkers pass across seeds 1 to 3 |
| 6 Soak and bench | `test/chaos -soak`, `*_bench_test.go` | containers | no invariant violation over hours; `benchstat` gate; mutation efficacy at or above 95 percent on `./billing` |

Build tags are the framework's. `task cover`, `task mutate`, `task bench`, `task fuzz`, and `task chaos-matrix` learn the billing packages and workloads (15.3); nothing runs in CI that is not one of those tasks.

### 14.2 Unit tier

Every handler is thin (3.2), so the unit tier tests the pure packages exhaustively and the handlers through the store fakes and the in-process fake Polar:

- **`entitle`**: `Decide` boundary tables (used 0, cap 0, `used + qty == cap`, `cap + 1`, unlimited, credits only, `overage` both ways, soft cap below and above allowance, thresholds at exact multiples and one below, quantity 1 and 10^12, overflow near `math.MaxInt64` rejected); `AssignPeriod` tables (instant at `starts_at`, at `ends_at - 1ns`, at `ends_at`, in grace, before grace, after open, with and without a grace period); `Crossings` (every subset of the six thresholds, a single event crossing several).
- **`rating`**: rounding tables at exactly half a minor unit and one pico-unit below and above, zero and negative quantities, credits partially and fully applied, several credits FIFO, `cap_amount` binding and not, `int64` overflow of `billable * unit_amount_pico` handled by `big.Int`, a meter with no price, the base line, a period with no rows.
- **`mirror`**: one table per Polar event type of Appendix C with the expected state and actions; `modified_at` older, equal, and newer; every account-resolution path; malformed payloads (missing `data`, wrong types) recorded as `failed` without panicking.
- **`polar`**: verification vectors (Standard Webhooks' published test vectors plus vectors generated here for the legacy scheme), multiple signatures, wrong secret, timestamp at the tolerance edge on both sides, non-numeric timestamp; `ParseAmount` tables (`"0"`, `"0.000000000001"`, `"0.0000000000001"` rejected, `"12.5"`, `"99999.999999999999"`, six integer digits rejected, negative rejected, exponent form rejected, leading plus rejected); the client's error typing per status; every wire type decoded from every sandbox fixture.
- **`hook`**: every early-exit path (method, size, headers, signature, timestamp, body shape), the principal it constructs, the status it returns for every `mediator.Code`.
- **`auth`**: JWKS fetch and cache, unknown `kid` refresh once, every algorithm, algorithm confusion, expired and not-yet-valid with leeway, missing subject, reserved claims stripped.
- **Handlers on memstore**: registry names and traits (like `TestRegistry_NamesAndOrder` of the example), `TestRequirements_Matrix`, routes and bindings, validation tables for every request (every tag rule at its boundary), and the behavior of every handler when the unit of work is absent (`ErrNoTransaction`) and when the store rejects (memstore's lock timeout and read-only errors).
- Every test that touches time uses the mediator clock or runs in a `synctest` bubble (the ingestor's backoff, the closer's schedule, the JWKS TTL, the webhook tolerance).

Coverage is measured as the framework measures it, with the `integration` and `faultinject` tags in the profile, and `tools/covergate` gates `billing/...` with the thresholds above.

### 14.3 Property and fuzz tier

Property tests with rapid:

| Test | Property |
|---|---|
| `PropCounter_EqualsSum` | For any sequence of record, adjust, and rejected-enforce operations on memstore, interleaved across goroutines, every counter equals the sum and count of its ledger rows (B3) |
| `PropDecide_Sound` | For any limits and `used`, `Decide` allows a quantity iff `used + qty <= cap` when a cap exists; `remaining` is never negative when a cap exists; `Decide` is monotone in `qty` |
| `PropCrossings_Complete` | For any `used` and `qty`, the crossings reported are exactly the thresholds in `(used, used + qty]` |
| `PropRating_Deterministic` | Permuting the rows, the credits, and the meter order of an input yields byte-identical statements and checksums (B8) |
| `PropRating_Monotone` | Adding a positive row never lowers a statement's total; adding an adjustment of `-q` after a row of `q` restores the previous total exactly |
| `PropRating_ExactWithinRounding` | Every line's `amount_minor` is within half a minor unit of the exact rational `billable * unit_amount_pico / 10^12` |
| `PropMirror_Commutes` | For any subscription lifecycle generated by the fake's model, any interleaving of its events with duplicates and reordering applied through `mirror.Apply` yields the state of the newest event (B6) |
| `PropPeriods_Partition` | For any random timeline of subscription starts, cycles, resets, cancellations, and revocations applied through the period rules, the account's periods are contiguous, non-overlapping, with exactly one open and at most one in grace, and every random instant maps to exactly one period (B7) |
| `PropIngestMapping_Limits` | For any attributes map within the request's limits, the mapped Polar event has at most 50 metadata keys of at most 40 characters and string values of at most 500 characters, and the reserved keys are never overwritten |
| `PropWebhookSignature_RoundTrip` | Any (id, timestamp, body, secret) signed by the fake verifies; flipping any byte of any of the four fails |
| `PropAllowance_NoDoubleCount` | For any plan and any interleaving of grant, cycle, and refresh events, a meter's allowance in a period equals the plan's included units or the mirrored grant's units (never both) plus operator credits plus the rollover remainders of earlier grants (5.6) |

Fuzz targets, corpora committed under `testdata/fuzz`:

- `FuzzWebhookVerify`: arbitrary headers and body never panic and never verify against a secret the input does not know (the fuzzer gets a fixed secret; acceptance requires the harness to have signed the input).
- `FuzzPolarEventDecode`: arbitrary bytes as a webhook body into `mirror.Apply` never panic; the outcome is `failed` or `ignored` for anything the fake did not produce.
- `FuzzParseAmount`: arbitrary strings never panic; every accepted string re-formats to a canonical decimal that parses to the same integer.
- `FuzzRating`: arbitrary inputs (bounded sizes) never panic and never overflow; the checksum is stable across two builds.
- `FuzzBillingRequestDecode`: arbitrary bytes into every billing request type through the framework's HTTP decoder never panic (the framework's `FuzzRequestDecode` extended with the billing registry).
- `FuzzAssignPeriod`: arbitrary instants and period pairs never panic and always return one of the two periods.

### 14.4 Fault sweep tier

The billing scenarios join `test/faultsweep` (3.2), because the catalogue test scans the whole module and the completeness gate reads one catalogue: `mediator/testkit/faultpoints.txt` gains the points of Appendix A, `test/faultsweep/plan`'s golden count moves accordingly, and the harness's node builder gains `nodeOpts{billing: true, polar: *polartest.Server}` so a cell's nodes register the billing module against a fresh schema, a fresh key prefix, and a fresh fake Polar. The framework's kinds, crash sweep, Postgres restart variant, and resource variants apply unchanged.

**Scenarios**:

| Scenario | Program | Points swept | Expected end state |
|---|---|---|---|
| `SweepRecordUsage` | `UpsertMeters` adds a fourth meter and `SetCaps` sets the hard cap that admits two events, both under the sweep; then two clients: A records three enforced events on that meter, B records the same event ID as A's second event at every step of A; then one `record`-mode event and one adjustment | `billing.usage.*`, `billing.period.*`, `billing.entitle.read`, `billing.meter.upsert`, `billing.cap.upsert`, `billing.notification.insert`, `billing.audit.insert`, `pg.tx.*`, `pg.idem.*`, `pg.outbox.*` | Exactly one row per event ID, the third enforced event rejected, counter equals the sum, one outbound row per API row, no row for the rejected event, one `cap_reached` notification, one audit row per change, byte-identical responses for A and B's shared ID; IB1, IB6, IB7 |
| `SweepIngest` | Twenty ledger rows across two accounts, one unlinked; the ingestor drains while `polar.ingest` fails every way, including ambiguous (the fake accepted, the reply was lost) and crash; then the account is linked | `billing.outbound.*`, `polar.ingest`, `pg.tx.*` | Every row `sent`, the fake holds every usage ID exactly once with `duplicates` equal to the re-sends, no `failed` row, the unlinked account's rows sent after the link; IB2 |
| `SweepWebhook` | The fake emits a subscription lifecycle (created, active, cycled, updated with a plan change, canceled, revoked) delivering each event three times and one pair out of order, with faults at every step of the edge, the command, the consumer, and the period moves | `billing.hook.*`, `billing.mirror.*`, `billing.period.*`, `billing.entitle.upsert`, `pg.inbox.*`, `pg.tx.*`, `redis.*` | Every webhook stored once and applied once, the mirror equal to the fake's subscription, periods contiguous with the right bounds, entitlements at the new plan, `SubscriptionChanged` published once per transition; IB3, IB4, IB8 (staleness of the summary after the change) |
| `SweepPeriodClose` | An account with usage in a period; the closer graces and closes it while late events, adjustments, and a `GrantCredit` fire at every step; the statement is built and reconciled against the fake's order | `billing.period.*`, `billing.statement.*`, `billing.credit.*`, `billing.reconcile.insert`, `polar.customer.state`, `polar.order.get`, `pg.tx.*` | One final statement whose rebuild matches, late rows in the open period, credits reduced FIFO exactly once, the reconciliation row `match`; IB4, IB5 |
| `SweepSync` | `OpenAccount` for two accounts, one whose email already exists in the fake under another external ID; `billing_sync` runs with faults at every step including ambiguous customer creation | `polar.customer.create`, `polar.customer.get`, `billing.account.*`, `pg.inbox.*` | The first account linked exactly once with one fake customer, the second `sync_state = failed` with no duplicate customer in the fake |
| `SweepPolarCommand` | `StartCheckout`, `OpenPortal`, `ChangePlan`, and `CancelSubscription` with faults at every step of the Polar call and the recording send | `polar.checkout.create`, `polar.subscription.update`, `polar.customersession.create`, `billing.checkout.insert`, `billing.mirror.write` | No duplicated checkout in the fake beyond what ambiguous outcomes explain, the mirror equal to the fake after the webhooks, the caller's retry after an ambiguous outcome converging on one state |
| `SweepCatalog` | `SyncCatalog` against a fake with three products (one archived, one whose metered price is on a meter unknown here), run twice, with faults at every step | `polar.products.list`, `billing.plan.upsert`, `pg.tx.*` | One plan version per product, `billing_plan_current` moved once per plan, the archived plan `archived`, the unmapped price reported once, the second run a no-op |
| `SweepShutdownBilling` | `SIGTERM` (modeled as the framework does) at every step of the ingestor, the closer, the webhook edge, and each billing consumer | every billing point | `Runtime.Run` returns nil, every acknowledged entry has an inbox row, no outbound row is `sent` without the fake holding it, no lease survives |

**Invariants** (package `billing/testkit`, shared with the chaos tier, run directly against Postgres, Redis, and the fake's state dump):

| ID | Check |
|---|---|
| IB1 | For every (account, meter, period), `billing_meter_period.quantity` and `events` equal the sum and count of `billing_usage` rows; `last_usage_id` is the greatest `usage_id` of them |
| IB2 | Every API row of a meter with a Polar event name has exactly one outbound row; after recovery none is `pending` older than the bound and none is `failed`; the set of `sent` usage IDs equals the fake's stored event `external_id`s; the fake's insert count per external ID is one; the sum of the fake's reported `duplicates` equals re-sends observed in the log |
| IB3 | For every subscription in the fake, `billing_subscription` equals its current object on the mirrored fields; every stored webhook has exactly one `applied_at`; every webhook the fake delivered exists in `billing_polar_event`; no `failed` outcome |
| IB4 | Per account, periods sorted by `starts_at` chain end to start with no gap or overlap from the account's creation to now; exactly one `open`, at most one `grace`; every ledger row's period contains `occurred_at`, or `late` is set and the row is in the period that was open when it was recorded, or `occurred_at >= ends_at` within the future window |
| IB5 | Every closed period has exactly one statement; rebuilding it from the ledger and the frozen allowance yields the stored lines and checksum; no ledger row of a closed period has `recorded_at` after `closed_at` |
| IB6 | For every ledger row written in enforce mode with `cap_at_write` set, `used_after <= cap_at_write`; per (account, meter, period), `used_after` over rows in `usage_id` order is strictly increasing by exactly each row's quantity (adjustments included) |
| IB7 | Per account, `source_event_id` is unique (the database enforces it; the check also matches the history: every `ok` record op has one row, every `fail` op none, every `info` op at most one) |
| IB8 | No cached summary or subscription read returned limits older than the last limits-changing command that returned before the read began, outside degraded windows (the framework's I7 over the billing history) |

Recovery, in every scenario, rebuilds the nodes, re-sends the keyed commands the clients had in flight, drains the ingestor against the fake, and waits for the consumers and the closer to go idle, bounded as the framework's harness bounds it.

### 14.5 Integration tier

Beside the code, with `//go:build integration` and each package's `TestMain` (testcontainers `postgres:18`, `redis:8`, and an in-process fake Polar):

- `billing`: every handler against a real store: the dedup paths of B2 including the post-expiry replay, the concurrent enforce test of B4 (64 goroutines, one cap, exactly `cap` units land), batch all-or-nothing (B15), adjustments and negative counters, period assignment at real boundaries, `WatchUsage` resume, the immutability trigger (B1), every consumer end to end through `redisx.Consumers` with three instances, cache invalidation after limit changes (B9), the statement rebuild (B8), and the migrations up, down, up.
- `billing/hook` with `httpapi` and F1: a real listener, signed and forged requests, the latency test of B10, and the drain (a webhook in flight during shutdown is answered, and one arriving after the listener stops is refused so Polar retries).
- `billing/ingest`, `billing/periods`, `billing/reconcile`: each component against real stores and the fake, including the failure table of 9.1 (429 with `Retry-After`, bisection on a poisoned batch, `waiting_customer` release).
- `billing/polar`: the conformance suite against the fake (always) and the sandbox (when `POLAR_SANDBOX_TOKEN` and `POLAR_SANDBOX_ORG` are set); fixture recording and comparison (10.9).

`test/integration/billing_*_test.go` runs `cmd/billingd` as a child process (built once in `TestMain`, one database and one key prefix per test, the fake Polar started by the test and pointed at the node's webhook URL) and covers: the end-to-end flow over HTTP (open account, wait for the link, record usage with and without a key, check entitlements, hit a cap, start a checkout that the fake completes, watch the subscription and periods change, cycle, close, statement, reconciliation match); readiness reflecting an ingest backlog when the fake is down and recovering when it returns; shutdown drain with in-flight `RecordUsage` and webhook requests (G16 for billing); the served OpenAPI document equal to `api/billing-openapi.json`.

### 14.6 Chaos tier

**Topology.** The compose `chaos` profile gains `polarfake` (from `cmd/polarfake`) reachable by the nodes only through per-node Toxiproxy listeners (`polar-node1` on `17070` to `polar-node5` on `17074`, added to `deploy/toxiproxy.json`), so the Polar side can be partitioned, slowed, and reset per node like Postgres and Redis. The fake delivers webhooks to the nodes directly at `http://nodeN:8080/billing/webhooks/polar`; inbound faults are the fake's own modes (10.9). The chaos node registers the billing module beside the workload when `CHAOS_APP` contains `billing` (with `POLAR_BASE_URL`, `POLAR_WEBHOOK_SECRET`, and the billing partitions from the environment) and exposes `/chaos/billing/...` admin endpoints for the controller (clock offset already exists; the billing ones reset the schema and expose the fake's URL).

**Workloads** (`test/chaos/billing_*.go`, one `scenario` implementation each):

| Name | Operations | Model and checker |
|---|---|---|
| `billing-usage` | Eight accounts, two meters, caps that the load reaches in about a minute; `record(account, meter, q)` in enforce mode with unique event IDs, `check(account, meter, q)`, `read(account, meter)` (uncached `GetUsage`) | Porcupine with the capped-counter model of Appendix G per (account, meter), `record` returning `used_after` on ok and `rejected` on the cap; IB1, IB6, IB7; L1 and LB1 |
| `billing-usage-retry` | As above, but a client whose op ended in `info` retries the same event ID until definite, across nodes | The history has no `info` at the end; every `ok` record has exactly one ledger row (IB7); Porcupine as above |
| `billing-ingest` | `billing-usage` load plus Polar nemeses; final: drain and compare | IB2 against the fake's state dump; LB1 (the queue drains within the bound after heal); `billing.ingest.protocol` is zero |
| `billing-subscriptions` | The fake runs a random lifecycle per account (activate, cycle, change plan, cancel, uncancel, revoke, resume) as signed webhooks while clients record usage and read `GetAccountSummary` and `GetSubscription`; nemeses include `webhook-storm` and `webhook-late` | IB3 against the fake; IB4; I6 on the `billing.polar` and `billing.subscriptions` streams (per-account order); the staleness checker of IB8 with degraded windows from the nemesis log; DLQ empty; no halted partition at the end |
| `billing-periods` | Clock skew and forced cycles drive periods through grace and close while usage, late events, and adjustments arrive; statements are built and reconciled | IB4, IB5, every closed period has one statement whose `billingctl statements rebuild` diff is empty, every reconciliation `match` |

**Nemeses added** to the framework's fifteen: `partition-polar` (Toxiproxy timeout toxic on the Polar proxies of one or all nodes), `polar-slow` (latency 1 to 8 s on the Polar proxies, so ingest batches time out mid-flight), `polar-5xx` and `polar-429` (the fake answers every call with 500 or 429 for the window), `polar-ambiguous` (the fake accepts ingests and drops the reply), `webhook-storm` (the fake re-delivers the last 100 webhooks in random order with duplicates), `webhook-late` (the fake holds a random half of new webhooks for 30 s, then delivers). Each holds the `polar` resource lock so at most one Polar-side nemesis is active.

**Checkers** in addition to the framework's: the workload checkers above, IB1 to IB8 read directly from the database and the fake's dump, LB1 (liveness: outbound drained, webhooks applied, periods current, consumer lag zero within `RecoveryBound`), and a log scan that also fails on `billing.statement.mismatch`, `protocol violation`, and `stale lease` from the sync consumer.

**Schedule, seeds, and reproducibility** are the framework's. `task chaos-matrix` adds the five billing workloads; the CI job's matrix gains five rows.

### 14.7 Soak and benchmarks

Soak: `billing-usage` and `billing-subscriptions` in `-soak` mode for two hours each with IB1, IB4, and IB6 checked every five minutes.

Benchmarks (all with `benchstat` against the baseline, gated at 10 percent on sec/op): `BenchmarkEntitleDecide` (0 allocs), `BenchmarkRating_1kRows`, `BenchmarkMirrorApply`, `BenchmarkWebhookVerify`, `BenchmarkIngestMapping`, and, under the `integration` tag against real Postgres, `BenchmarkRecordUsage_Tx` (one account, one meter, sequential), `BenchmarkRecordUsage_Parallel` (64 accounts), `BenchmarkCheckEntitlement`, and `BenchmarkHTTP_RecordUsage` through a real listener. B16's numbers are asserted in the integration benchmarks as p50 and p99 over 10,000 operations, on the reference stack only (`BENCH_REFERENCE=1`), and reported elsewhere.

### 14.8 Gates and the test-to-code ratio

Gates: coverage (14.1), mutation efficacy at or above 95 percent for `./billing` (`task mutate` runs gremlins on `./mediator` and `./billing` in sequence; survivors are triaged as design-notes 8.14 did), fault sweep completeness over the extended catalogue, a green billing chaos matrix, and the benchmark gate. The test-to-code ratio of `billing/...` is reported in the CI summary beside the framework's.

---

## 15. Docker, local development, and CI

### 15.1 Compose

`deploy/docker-compose.yml` gains:

- Profile `billing`: `billingd` (Dockerfile target `billingd`, port `8081`, environment as 12.4 with `POLAR_BASE_URL=http://polarfake:8090` and a fixed fake webhook secret unless `POLAR_ACCESS_TOKEN` is set in the host environment, in which case the sandbox URL and the host's secrets are passed through) and `polarfake` (target `polarfake`, port `8090`, delivering webhooks to `http://billingd:8081/billing/webhooks/polar`).
- Profile `chaos`: `polarfake` behind Toxiproxy with the five listeners of 14.6; `node1..3` get `CHAOS_APP=workload,billing`, `POLAR_BASE_URL=http://toxiproxy:1707N`, and the fake's secret.

`deploy/Dockerfile` gains the targets `billingd` (with `cmd/billingctl` beside it) and `polarfake`. The chaos node target is unchanged; it already carries the fault tag.

### 15.2 Task targets

| Target | Does |
|---|---|
| `task billing-up` / `task billing-down` | Postgres, Redis, `polarfake`, and `billingd` on `:8081` |
| `task openapi` / `task openapi-check` | now export and check both `api/openapi.json` (through `mediatorctl`) and `api/billing-openapi.json` (through `billingctl`) |
| `task cover` | `-coverpkg=./mediator/...,./billing/...` over both trees; `coverThresholds` gains the billing packages' thresholds |
| `task mutate` | gremlins on `./mediator` then `./billing`, both at 95 |
| `task bench` / `task bench-baseline` | `./mediator/...` and `./billing/...` |
| `task fuzz` | discovers the billing targets like any other |
| `task test-sweep` | unchanged; the billing scenarios are in the same package |
| `task chaos -workload billing-usage` and `task chaos-matrix` | the billing workloads |
| `task polar-conformance` | the sandbox conformance run; fails fast without `POLAR_SANDBOX_TOKEN` |

Every target stays a `go run ./tools/task <name>` with its Makefile alias, and `tools/task/app_test.go` asserts the exact commands.

### 15.3 CI

The workflow of `.github/workflows/ci.yml` stays manual-dispatch only (design-notes 6). Every dispatch's short jobs already run `./...`, so the billing unit, property, short fuzz, integration, and openapi checks join them without new jobs; the openapi job additionally type-checks the billing TypeScript client. With `long=true`: the sweep includes the billing scenarios, mutation runs both trees, the benchmark gate covers both, and the chaos matrix gains the five billing rows. A new `polar-conformance` job runs only when dispatched with `conformance=true` and the repository secrets `POLAR_SANDBOX_TOKEN` and `POLAR_SANDBOX_ORG` exist; it uploads the fixture diff as an artifact and fails when a fixture changed, which is the signal to review Polar's change and re-record.

---

## 16. Milestones and acceptance

Each milestone is done only when its tests are green locally with the commands of `docs/handoff.md` section 4 and, when dispatched, in CI. Estimates are for one engineer.

| # | Milestone | Deliverables | Acceptance |
|---|---|---|---|
| BM0 | Scaffold | F1 and F2 with their design-notes rows and unit rows; `billing/` layout with every request, event, and consumer type registered (handlers returning `CodeInternal` "not implemented"); `billing/migrations/0001`; `cmd/billingd` and `cmd/billingctl` that build, migrate, and serve; `api/billing-openapi.json` exported and drift-checked; the TypeScript client job; `task cover`, `mutate`, `bench`, `openapi` extended; `TestRequirements_Matrix`, `TestSystemRequests_UnreachableOverHTTP`, `TestBillingOpenAPI_MatchesCommitted` | `task test`, `task openapi-check`, and `task cover` green with the new packages at their thresholds (the pure packages are empty shells at 100 percent); `billing-up` serves `/billing/docs` |
| BM1 | Ledger and entitlement | `entitle`, accounts, meters, `RecordUsage`, `RecordUsageBatch`, `AdjustUsage`, `SetCaps`, `CheckEntitlement`, calendar periods and the closer's grace and open steps, `GetUsage`, `ListUsageEvents`, `WatchUsage`, `GetAccountSummary`, notifications for thresholds, audit; the immutability trigger; `SweepRecordUsage`; IB1, IB4, IB6, IB7; integration tests of B1 to B4, B7, B13 to B15 | Tiers 1 to 4 green; the sweep completeness gate green with the new points; `BenchmarkRecordUsage_Tx` baseline recorded |
| BM2 | Polar outbound | `billing/polar` client and `polartest` with the conformance suite; sandbox fixtures recorded once by hand (`task polar-conformance`); `billing_sync`, `LinkPolarCustomer`, the ingestor, `GetIngestStatus`; `SweepIngest`, `SweepSync`; IB2; `billing-ingest` chaos workload with the Polar nemeses | B5 and B11 verified in tiers 3 to 5; the conformance suite green against the fake and the sandbox; question 3 and 5 of 17.2 answered and recorded |
| BM3 | Polar inbound | F1 in use; the webhook edge; `ApplyPolarEvent`; `mirror` and `billing_mirror`; subscription periods and boundary moves; credits from benefit grants; the catalog sync and plan mirror; `billing_entitlements` and `RefreshEntitlements`; the synchronous Polar commands (`StartCheckout`, `OpenPortal`, `ChangePlan`, `CancelSubscription`, `UncancelSubscription`); `SweepWebhook`, `SweepPolarCommand`, `SweepCatalog`; IB3, IB8; `billing-subscriptions` chaos workload with `webhook-storm` and `webhook-late` | B6, B9, B10 verified in tiers 3 to 5; `PropMirror_Commutes` and `PropPeriods_Partition` green; end-to-end integration flow through checkout and cycle |
| BM4 | Statements and reconciliation | `rating`; `ClosePeriod`, `BuildStatement`, `billing_statements`; `billing_reconcile`, the reconciler, `Reconcile`; `billingctl statements rebuild`; `SweepPeriodClose`; IB5; `billing-periods` chaos workload | B8, B12, B13 verified; every closed period in a chaos run has a matching statement and a `match` reconciliation |
| BM5 | Chaos matrix and shutdown | `SweepShutdownBilling`; the five billing workloads in `task chaos-matrix` and the CI matrix; soak mode | Every billing workload green at seeds 1 to 3 for 20 minutes each; a two-hour soak of `billing-usage` and `billing-subscriptions` without a violation; B12 and G16 for billing |
| BM6 | Hardening | Mutation gate at 95 on `./billing` with the triage recorded; `benchstat` baseline and gate; B16 measured on the reference stack; `billingctl` complete; `docs/billing-handoff.md` and the runbook sections of 9.1 to 9.4 checked against a real incident drill (Polar down for an hour in compose) | All gates on; every row of section 13 has a green test; release candidate |

Definition of done for billing v1.0: every guarantee in section 13 has a passing test, every gate of 14.8 is enforced, the billing chaos matrix has been green across three seeds on two consecutive dispatches, the Polar conformance suite is green against the sandbox on the recorded fixtures, and `billingd` runs in the compose stack with its OpenAPI document consumed by the generated TypeScript client in CI.

---

## 17. Decision log and open questions

### 17.1 Decisions

| Decision | Alternatives considered | Reason |
|---|---|---|
| The ledger is the source of truth for usage; Polar's meters are downstream | Ingest into Polar only and read usage back from Polar; treat Polar's meters as the truth | Entitlement decisions must be atomic with recording and cannot wait on a third party; Polar's events are immutable but its meters are not queryable at transaction speed; and a reconciled mirror is what proves an invoice right |
| Counters maintained in the recording transaction, not by a projector | A `UsageRecorded` durable event and a read-model consumer | Atomicity with the cap decision needs the counter under the same lock; a projector would make the counter eventually consistent and the cap unenforceable; it also halves the write volume |
| No per-usage durable event | Publish `UsageRecorded` for every row | Nothing downstream needs it: the queue row is written in the transaction, the audit is the ledger, and thresholds are rare; the outbox stays cheap for the events that carry meaning |
| Polar customer created asynchronously by a consumer | Create it in `OpenAccount`; create it lazily at first ingest or checkout | A third-party call does not belong in a request transaction (B11); `OpenAccount` must succeed when Polar is down; the `waiting_customer` state makes the delay harmless |
| Synchronous Polar commands are `NoUnitOfWork` and record through a nested system command | Run them in a unit of work and call Polar inside it; queue them and poll | The caller needs the URL now; a transaction held across a third-party call is the failure the framework's design avoids; every one of these operations is reported back by a webhook, so nothing can be lost |
| Webhook applied by a consumer, not in the edge | Apply the mirror change in `ApplyPolarEvent` | Polar's timeout is 10 seconds and its recommendation 2; the consumer gets ordering per account, the inbox, retries, and dead-lettering for free; the edge stays two statements |
| `StrictOrder(true)` on the mirror consumer | Dead-letter and continue | A skipped subscription event leaves every later event applied to a wrong base; a halted partition with an operator command is the safer failure |
| `modified_at` guard with delivery order for ties | Event-type ranking for ties; vector clocks | Polar's own retries are the only source of equal instants and they are identical; ranking would encode assumptions about Polar's clock |
| Periods follow Polar's cycle when subscribed, calendar months otherwise, with grace and late rows | Always calendar; always Polar; assign rows by `recorded_at` | Statements must match Polar's order periods to reconcile; unsubscribed accounts still need periods for allowances; grace keeps late events accurate without leaving periods open forever |
| Integer prices in 10^-12 of a minor unit, `math/big` for products, one rounding per line | `NUMERIC` in Postgres; float64; decimal library | Exactness with no dependency; pgx's numeric handling would put arithmetic in SQL where it is hard to test; the rounding rule is stated once and property-tested |
| `external_id = usage_id` on every Polar event | No `external_id`; a batch-level key | Polar deduplicates per event on `external_id`, which is the only thing that makes at-least-once sending exact |
| Adjustments not ingested | Send negative-quantity events | Whether Polar's sum aggregation admits negative values, and what its portal shows for them, is unverified; operators apply refunds and credits in Polar until question 4 is answered |
| One table for the ledger in v1, with a stated partitioning path | Range partitions from day one | Partitioned tables cannot carry the dedup unique index without the partition key; the side-table design is known and can be added by migration when the size demands it, without touching invariants |
| Billing sweep scenarios live in `test/faultsweep` and points in the shared catalogue | A separate sweep package and catalogue | The catalogue test scans the module and the completeness gate reads one file; two catalogues would need a second gate and a scan filter for no benefit |
| The fake Polar is one implementation used in process, in compose, and in chaos, kept honest by a conformance suite against the sandbox | A mock per test; recorded HTTP cassettes | The chaos tier needs a live counterpart with fault modes; cassettes cannot model dedup or webhooks; the conformance suite is the `storetest` idea applied to a third party |
| 403 for a non-owner, never 404 | 404 to hide existence | IDs are unguessable UUIDv7 values; one code keeps the requirement matrix and the client simple |
| Better Auth JWTs verified by JWKS with EdDSA and ES256; HS256 only with an explicit secret | Session lookups against the auth server; opaque tokens | The framework's authenticator is a pure function of the request; a JWKS keeps it that way and matches Better Auth's JWT plugin; a network call per request would be a new failure mode |
| `past_due` does not suspend | Suspend on `past_due`; suspend on `unpaid` | Whether to keep serving a customer whose card failed is a product policy with a dunning window; the status is mirrored and notified, and `SuspendAccount` exists for the policy to call |
| F1 as a mount option rather than an authenticator that rewrites the body | The authenticator trick, which needs no framework change | An authenticator that changes the request body is a trap; the mount is two lines in the framework and honest |
| The Polar API version is pinned by header and recorded with the fixtures | Follow Polar's current version | The current version changes every quarter and the webhook payloads with it; a pinned version turns a Polar change into a deliberate fixture re-record |

### 17.2 Open questions

These do not block BM0 or BM1. Defaults are stated; change them by amending this document.

1. **Tiered and volume pricing.** Polar supports more than one unit price per meter through tiers. Default: unsupported; the catalog sync records a plan with tiered prices as `unmapped` for that meter and `billingctl catalog check` reports it. Candidate for v1.1 with a `billing_plan_tier` table and a rating extension.
2. **Meter aggregations beyond sum and count.** Default: a Polar meter with `max`, `unique`, or `average` cannot be mirrored with allowances; such meters are ingested (if configured) but have no entitlement row. Revisit when a product needs one.
3. **The scale of Polar's `unit_amount` on metered prices.** Polar's API reference (2026-09-27) describes it as a decimal string in cents with up to twelve decimals and at most five integer digits, which is why prices are stored at 10^-12 of a cent (5.7). The sandbox fixture at BM2 confirms it; `ParseAmount` fails loudly on a price finer than that, and the scale is then changed in one place.
4. **Adjustments in Polar.** Default: not ingested (17.1). If the sandbox shows that negative `metadata.quantity` values are summed and displayed sensibly, adjustments become ingestible with `external_id = usage_id` like any row.
5. **Ingest batch atomicity.** Whether one invalid event rejects the whole batch (422) or only itself. The bisection of 9.1 handles both; the fixture at BM2 records which, and the bisection can then be simplified.
6. **Customer creation before ingest.** Whether Polar accepts an event for an `external_customer_id` it has never seen. Default: assume not; the `waiting_customer` state and `billing_sync` guarantee the customer exists first. If the sandbox accepts unknown IDs, the state stays (linking is still needed for checkout and state) but the ingestor may stop waiting.
7. **Polar's rounding.** Reconciliation tolerates half a minor unit per line (5.7). If Polar rounds per event or per unit rather than per line, the tolerance is widened to what the fixtures show and the difference is documented on `StatementView`.
8. **Organization membership changes.** A user who leaves an organization keeps nothing; a user whose active organization changes between requests records usage on whichever account the token names. Default: the auth server's token is trusted; no membership table here.
9. **Retention of the ledger.** Never deleted in v1. A future archive step (partition detach to cold storage) is the natural follow-up to the partitioning path of 5.3.
10. **Reference stack for B16.** The framework's question 5 applies; until CI hardware is chosen, the numbers are asserted only with `BENCH_REFERENCE=1` and the gate is relative.

---

## Appendix A. Billing fault points

Added to `mediator/testkit/faultpoints.txt` (the shared catalogue; `TestFaultPointCatalogue` scans the whole module). The file is whitespace-separated tokens, so the block below is what goes in verbatim:

```
billing.account.insert     billing.account.update     billing.account.link
billing.usage.dedup        billing.usage.counter      billing.usage.insert
billing.usage.outbound     billing.usage.notify
billing.period.read        billing.period.insert      billing.period.transition   billing.period.allowance
billing.entitle.read       billing.entitle.upsert
billing.cap.upsert         billing.credit.upsert      billing.credit.consume
billing.hook.insert
billing.mirror.read        billing.mirror.write       billing.mirror.mark
billing.checkout.insert    billing.order.upsert       billing.plan.upsert      billing.meter.upsert
billing.statement.insert
billing.notification.insert  billing.audit.insert     billing.eventlog.insert  billing.reconcile.insert
billing.outbound.select    billing.outbound.mark      billing.outbound.attempt billing.outbound.release
polar.ingest               polar.customer.create      polar.customer.get       polar.customer.state
polar.customersession.create                          polar.checkout.create    polar.subscription.update
polar.products.list        polar.order.get
```

Forty-three points. Nine of them also call `testkit.FaultAfter` after the operation, so the ambiguous and crash kinds act there: `billing.usage.insert`, `billing.hook.insert`, `billing.mirror.mark`, `billing.statement.insert`, `billing.outbound.mark`, `polar.ingest`, `polar.customer.create`, `polar.checkout.create`, `polar.subscription.update`. `plan.AfterPoints` finds those sites from the sources, so the ambiguous kind is demanded exactly where it can act, and `test/faultsweep/zz_completeness_test.go` gains no billing exclusion: every point is reached by at least one scenario of 14.4.

## Appendix B. Requests, routes, classes, and traits

| Request | Method and path | Class | Idempotency key | Cache tags read / bumped | Retry | Unit of work |
|---|---|---|---|---|---|---|
| `OpenAccount` | `POST /billing/accounts` 201 | self-service, admin | header | / `account` | 3 | yes |
| `SuspendAccount` | `POST /billing/accounts/{accountId}/suspend` | admin | header | / `account`, `sub` | | yes |
| `ReinstateAccount` | `POST /billing/accounts/{accountId}/reinstate` | admin | header | / `account`, `sub` | | yes |
| `CloseAccount` | `POST /billing/accounts/{accountId}/close` | self-service, admin | header | / `account`, `sub` | | yes |
| `SetCaps` | `PUT /billing/accounts/{accountId}/caps/{meterKey}` | self-service | header | / `account` | | yes |
| `RecordUsage` | `POST /billing/usage` | service | body `eventId` | | 3 | yes |
| `RecordUsageBatch` | `POST /billing/usage/batch` | service | body `batchId` | | 3 | yes |
| `AdjustUsage` | `POST /billing/accounts/{accountId}/adjustments` | admin | body `adjustmentId` | | 3 | yes |
| `GrantCredit` | `POST /billing/accounts/{accountId}/credits` 201 | admin | header | / `account` | | yes |
| `StartCheckout` | `POST /billing/accounts/{accountId}/checkout` 201 | self-service | none (client retries) | | | no |
| `OpenPortal` | `POST /billing/accounts/{accountId}/portal` 201 | self-service | none | | | no |
| `ChangePlan` | `POST /billing/accounts/{accountId}/subscription/plan` | self-service | none | | | no |
| `CancelSubscription` | `POST /billing/accounts/{accountId}/subscription/cancel` | self-service | none | | | no |
| `UncancelSubscription` | `POST /billing/accounts/{accountId}/subscription/uncancel` | self-service | none | | | no |
| `ApplyPolarEvent` | mount `POST /billing/webhooks/polar` (rpc route unused) | webhook | body `webhookId` | | | yes |
| `SyncCatalog` | `POST /billing/catalog/sync` | admin | none | | | no |
| `UpsertMeters` | `PUT /billing/meters` | admin | header | / `catalog` | | yes |
| `ClosePeriod` | `POST /billing/periods/{periodId}/close` | admin, system | header | | 3 | yes, serializable |
| `Reconcile` | `POST /billing/accounts/{accountId}/reconcile` | admin, system | none | | | no |
| `LinkPolarCustomer` | rpc | system | | / `account`, `sub` | | yes |
| `UpsertCatalog` | rpc | system | | / `catalog` | | yes |
| `RefreshEntitlements` | rpc | system | | / `account`, `sub` | 3 | yes |
| `OpenPeriod`, `GracePeriod` | rpc | system | | | 3 | yes, serializable |
| `BuildStatement` | rpc | system | | | 3 | yes, serializable |
| `ApplyPolarObject` | rpc | system | | / `account`, `sub` | | yes |
| `RecordNotification` | rpc | system | | | | yes, `RequiresNew` |
| `RecordCheckout`, `RecordReconciliation` | rpc | system | | | | yes |
| `GetAccount` | `GET /billing/accounts/{accountId}` | self-service | | `account` / | | read-only |
| `ResolveAccount` | `GET /billing/accounts/by-external/{kind}/{externalId}` | self-service, admin | | | | read-only |
| `GetAccountSummary` | `GET /billing/accounts/{accountId}/summary` | self-service | | `account`, `sub` (30 s) / | | read-only |
| `CheckEntitlement` | `GET /billing/accounts/{accountId}/entitlements/{meterKey}` | self-service, service | | never | | read-only |
| `GetUsage` | `GET /billing/accounts/{accountId}/usage` | self-service | | | | read-only |
| `ListUsageEvents` | `GET /billing/accounts/{accountId}/usage/events` | self-service | | | | read-only |
| `WatchUsage` | `GET /billing/accounts/{accountId}/usage/stream` (SSE) | self-service | | | | none |
| `GetSubscription` | `GET /billing/accounts/{accountId}/subscription` | self-service | | `sub` / | | read-only |
| `ListPlans`, `GetPlan`, `ListMeters` | `GET /billing/plans`, `/billing/plans/{planKey}`, `/billing/meters` | authenticated | | `catalog` / | | read-only |
| `ListPeriods` | `GET /billing/accounts/{accountId}/periods` | self-service | | | | read-only |
| `GetStatement`, `ListStatements` | `GET /billing/statements/{statementId}`, `/billing/accounts/{accountId}/statements` | self-service | | | | read-only |
| `ListCredits`, `ListNotifications` | `GET /billing/accounts/{accountId}/credits`, `/notifications` | self-service | | | | read-only |
| `GetPolarEvent`, `ListPolarEvents` | `GET /billing/polar/events/{webhookId}`, `/billing/polar/events` | admin | | | | read-only |
| `ListReconciliations` | `GET /billing/accounts/{accountId}/reconciliations` | admin | | | | read-only |
| `GetIngestStatus` | `GET /billing/polar/ingest` | admin | | | | read-only |

Every request implements `Named` (`billing.<Type>`), `Requires`, and `Describe`. Rate limits: `CheckEntitlement` 600 per minute per subject with burst 100; `StartCheckout` and `OpenPortal` 10 per minute per subject.

## Appendix C. Polar webhooks and mirror actions

`mirror.Apply(state, event)` returns the new state and a list of actions the consumer executes. Fields named are the ones this system reads; everything else in the payload is stored opaquely.

| Event type | Fields read | Mirror effect | Actions |
|---|---|---|---|
| `customer.created`, `customer.updated` | `data.id`, `data.external_id`, `data.email`, `data.modified_at` | link the account when unlinked (10.5) | `LinkPolarCustomer` when newly linked |
| `customer.deleted` | `data.id` | note on the account (`sync_state = failed`, reason) | none |
| `customer.state_changed` | `data.external_id`, `data.active_subscriptions[]`, `data.active_meters[]`, `data.granted_benefits[]` | stored for reconciliation; no mirror change (the specific events below carry the authoritative changes) | none |
| `checkout.created`, `checkout.updated`, `checkout.expired` | `data.id`, `data.status`, `data.external_customer_id` or `data.customer.external_id`, `data.product_id`, `data.subscription_id`, `data.expires_at`, `data.modified_at` | upsert `billing_checkout` | none |
| `subscription.created` | `data.id`, `data.status`, `data.customer.id`, `data.customer.external_id`, `data.product_id`, `data.current_period_start`, `data.current_period_end`, `data.cancel_at_period_end`, `data.started_at`, `data.modified_at` | upsert the subscription row (guarded) | none until active |
| `subscription.active`, `subscription.resumed` | as above | status and period; the account's periods move to the subscription cycle (5.4); a resume from `paused` starts a new Polar billing period, so it moves them like a cycle | `SubscriptionChanged`; `GracePeriod` and `OpenPeriod` |
| `subscription.uncanceled` | as above | `cancel_at_period_end` cleared; periods unchanged (nothing moved at cancel) | `SubscriptionChanged` |
| `subscription.cycled` | as above | new current period; Polar grants whose `granted_at` falls in the new period are re-bound to it (5.6) | `SubscriptionChanged`; `GracePeriod` and `OpenPeriod`; credits with `rollover = false` bound to the old period expire |
| `subscription.updated` | as above | any field change; a product change updates `plan_key`; a period change moves periods | `SubscriptionChanged` when status, product, or period changed |
| `subscription.canceled` | plus `data.canceled_at`, `data.ends_at` | mirrored; periods unchanged (the subscription runs to `ends_at`) | `SubscriptionChanged` |
| `subscription.revoked` | plus `data.ended_at` | status; the subscription period ends at `ended_at` and a calendar period opens | `SubscriptionChanged`; `GracePeriod` and `OpenPeriod` |
| `subscription.past_due`, `subscription.paused` | as above | status | `SubscriptionChanged` |
| `subscription.migrated` | as above | product and plan | `SubscriptionChanged` |
| `order.created`, `order.updated`, `order.paid`, `order.refunded` | `data.id`, `data.status`, `data.billing_reason`, `data.subscription_id`, `data.total_amount`, `data.currency`, `data.items[]`, `data.period_start`/`period_end` when present, `data.paid_at`, `data.modified_at` | upsert `billing_order` | `RecordReconciliation` (system, `kind = statement`) computed from the mirrored order when a statement already exists for its period; otherwise the `billing_reconcile` consumer finds the order when the statement is finalized (8.3) |
| `refund.created`, `refund.updated` | `data.order_id`, `data.amount`, `data.status` | note on the order | none |
| `benefit_grant.created`, `benefit_grant.updated` | `data.id`, `data.customer.external_id`, `data.benefit.type` (`meter_credit`), `data.benefit.properties.meter_id`, `.units`, `.rollover`, `data.subscription_id`, `data.granted_at`, `data.is_granted`, `data.modified_at` | for `meter_credit`: upsert the credit row of this grant bound to the account's period containing `granted_at` (5.6) | `CreditGranted` when newly granted |
| `benefit_grant.cycled` | as `benefit_grant.created` | for `meter_credit`: a new credit row for the new cycle, bound to the period containing `granted_at`; the previous cycle's row keeps its `remaining` when `rollover` is set and is otherwise expired | `CreditGranted` |
| `benefit_grant.revoked` | `data.id`, `data.revoked_at` | the credit's `remaining` becomes 0 (a new row is not needed; `remaining` and `period_id` are the mutable columns and their changes are audited) | `CreditGranted` with `units = 0` so entitlements refresh |
| `product.created`, `product.updated`, `benefit.created`, `benefit.updated` | `data.id` | `UpsertCatalog` for the product (or the products of the benefit) | `CatalogSynced` |
| `discount.*`, `organization.updated` | | stored, `ignored` | none |

Every event is stored before it is applied; the outcome column records `applied`, `stale`, `unmatched`, `ignored`, or `failed`.

## Appendix D. Ingest event mapping

```json
{
  "name": "tokens_in",
  "external_customer_id": "user_01J9X...",
  "external_id": "0192e1b4-7c2a-7f3e-9b1d-3a4c5e6f7a8b",
  "timestamp": "2026-09-27T14:03:21.412Z",
  "external_member_id": "user_01J9Y...",
  "metadata": {
    "quantity": 1532,
    "meter": "tokens_in",
    "usage_id": "0192e1b4-7c2a-7f3e-9b1d-3a4c5e6f7a8b",
    "period_id": "0192e0aa-...",
    "account_id": "0192dfff-...",
    "mode": "enforce",
    "model": "gpt-x"
  }
}
```

One `POST /v1/events/ingest` carries `{"events": [ ... ]}` with up to `INGEST_BATCH` of these; the response `{"inserted": n, "duplicates": m}` must satisfy `n + m == len(events)`.

## Appendix E. Hot-path SQL

### E.1 `RecordUsage` (enforce mode)

Inside the framework's unit of work (`READ COMMITTED`, read-write), after validation and idempotency reservation:

```sql
-- 1. Account and meter (no locks).
SELECT status, polar_customer_id, external_id FROM billing_account WHERE account_id = $1;
SELECT aggregation, polar_event_name, status FROM billing_meter WHERE meter_key = $1;

-- 2. Ledger dedup beyond the idempotency window.
SELECT usage_id, request_hash FROM billing_usage WHERE account_id = $1 AND source_event_id = $2;
--    found and hash equal: return {usageId, deduplicated: true, counters as of now}; hash differs: idempotency_mismatch

-- 3. Periods: the open one and, if any, the grace one.
SELECT period_id, starts_at, ends_at, status FROM billing_period
WHERE account_id = $1 AND status IN ('open', 'grace');
--    entitle.AssignPeriod picks the period and sets late

-- 4. Limits (no lock) and the counter (locked, created on first use).
SELECT included, credits, overage, hard_cap, soft_cap FROM billing_entitlement
WHERE account_id = $1 AND meter_key = $2 AND period_id = $3;
INSERT INTO billing_meter_period (account_id, meter_key, period_id) VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
SELECT quantity FROM billing_meter_period
WHERE account_id = $1 AND meter_key = $2 AND period_id = $3 FOR UPDATE;
--    entitle.Decide(limits, quantity, qty): rejected -> return precondition quota_exceeded (the transaction rolls back)

-- 5. Write.
UPDATE billing_meter_period
SET quantity = quantity + $4, events = events + 1, last_usage_id = $5, updated_at = now()
WHERE account_id = $1 AND meter_key = $2 AND period_id = $3
RETURNING quantity;
INSERT INTO billing_usage (usage_id, account_id, meter_key, period_id, quantity, occurred_at, late, mode, source,
    source_event_id, request_hash, attributes, used_after, cap_at_write, request_id, correlation_id, subject, node)
VALUES ($5, $1, $2, $3, $4, $6, $7, 'enforce', 'api', $8, $9, $10, $11, $12, $13, $14, $15, $16);
INSERT INTO billing_polar_outbound (usage_id, account_id, external_customer_id, event_name, occurred_at, metadata, state)
VALUES ($5, $1, $17, $18, $6, $19, CASE WHEN $20 THEN 'pending' ELSE 'waiting_customer' END);   -- only when the meter has an event name
SELECT pg_notify('billing_outbound', '');                                                          -- once per transaction, through Tx.Notify

-- 6. Threshold crossings: mediator.Publish(UsageThresholdCrossed{...}) per crossing (outbox rows in this transaction).
```

Fault points, in order: `billing.usage.dedup`, `billing.period.read`, `billing.entitle.read`, `billing.usage.counter`, `billing.usage.insert` (+After), `billing.usage.outbound`, `billing.usage.notify`.

Lock analysis: statement 4 takes the only row lock; a concurrent enforced write of the same (account, meter, period) blocks there and, when it proceeds, reads the updated `quantity`, so `Decide` always sees the committed counter (B4). Statement 2 and the unique constraint of `billing_usage_dedup` together make a race between two first-time writers of one event ID end with one row and one `unique_violation`, which the handler maps to the dedup path by re-reading (the second writer's transaction is then committed as a no-op after the framework's idempotency row records the same response; when the two writers race inside the idempotency window, the framework's row lock has already serialized them and this branch is never reached).

### E.2 `ApplyPolarEvent`

```sql
INSERT INTO billing_polar_event (webhook_id, event_type, sent_at, external_id, account_id, payload)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (webhook_id) DO NOTHING
RETURNING 1;                                                   -- no row: duplicate, return
-- mediator.Publish(PolarEventReceived{WebhookID, Type, ExternalID, AccountID}) with StreamKey per 7.2.5
```

Fault points: `billing.hook.insert` (+After), then the framework's `pg.outbox.*`.

### E.3 Mirror apply of a subscription event (consumer transaction)

```sql
SELECT payload FROM billing_polar_event WHERE webhook_id = $1;                          -- billing.mirror.read
SELECT ... FROM billing_subscription WHERE subscription_id = $2 FOR UPDATE;             -- current mirror state (may be absent)
-- mirror.Apply(state, event) -> new state, actions, outcome
INSERT INTO billing_subscription (...) VALUES (...)
ON CONFLICT (subscription_id) DO UPDATE SET ... , polar_modified_at = EXCLUDED.polar_modified_at, applied_webhook_id = $1
WHERE billing_subscription.polar_modified_at <= EXCLUDED.polar_modified_at;              -- billing.mirror.write
-- actions: nested Send(GracePeriod), Send(OpenPeriod), Publish(SubscriptionChanged), Publish(CreditGranted)
UPDATE billing_polar_event SET applied_at = now(), outcome = $3 WHERE webhook_id = $1;   -- billing.mirror.mark (+After)
```

The `FOR UPDATE` on the subscription row serializes a synchronous `ApplyPolarObject` with a webhook of the same subscription; the `WHERE` on `polar_modified_at` is the guard of 5.9 in SQL, and Postgres locks the conflicting row even when the `WHERE` rejects the update, so the two paths cannot interleave.

## Appendix F. Standard Webhooks verification

Headers: `webhook-id` (the message ID, identical across retries of one message), `webhook-timestamp` (Unix seconds), `webhook-signature` (one or more space-separated `v1,<base64>` entries).

```
secret   = base64-decode of the configured secret after stripping a "whsec_" prefix   (Standard Webhooks)
           for a legacy Polar secret (created before 2026-09-08): the key is the UTF-8 bytes of the full "whsec_..." string as shown
           in the dashboard, undecoded (Polar's earlier scheme; a Standard Webhooks library would need it base64-encoded first, this verifier does not)
signed   = webhook-id + "." + webhook-timestamp + "." + raw body bytes
expected = base64( HMAC-SHA256(secret, signed) )
accept   iff some v1 entry equals expected under a constant-time compare
       and |now - webhook-timestamp| <= tolerance (5 minutes)
```

Both secrets may be configured during a rotation; every signature entry is checked against every configured secret. The verifier never logs the body or the secret. Test vectors: the Standard Webhooks reference vectors, plus vectors generated by `polartest` for both schemes and committed under `billing/polar/testdata/webhooks/`.

## Appendix G. History records and the capped-counter model

Billing workloads record the framework's history format (spec Appendix C) with these functions:

| `f` | `key` | invoke `value` | ok `value` |
|---|---|---|---|
| `record` | `<account>/<meter>` | `{"q": n, "event": id}` | `{"used": usedAfter}`; a cap rejection is `ok` with `{"rejected": true}` because it is a definite, observable outcome |
| `check` | `<account>/<meter>` | `{"q": n}` | `{"allowed": bool, "used": n}` (informational; not part of the model) |
| `read` | `<account>/<meter>` | | `{"used": n}` |
| `limits` | `<account>` | | the summary's limits (for the staleness checker) |

Porcupine model per key (the capped counter): state `used`; `record(q)` transitions to `used + q` and returns `{used: used + q}` when `used + q <= cap`, else stays and returns `{rejected: true}`; `read` returns `used`. `info` records are treated as the framework treats them (an operation that may complete at any later time). The model is partitioned by key like the register model, and the cap is the workload's configured cap for that (account, meter), constant for the run. A history is linearizable under this model iff every enforced write took effect atomically at some instant between its invoke and return and the counter never exceeded the cap, which is B4 and B3 observed from outside.
