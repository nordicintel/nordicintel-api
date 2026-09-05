
# nordicintel-api — Project & Technical Specification

A unified HTTP API that acts as a **router/hub** over multiple upstream statistical
data providers (SCB / PxWeb-family endpoints, and more later), exposing them all
through a single interface conforming to the **[PxApi 2.0](https://statistikdatabasen.scb.se/api/v2/swagger/v2/swagger.json)** contract.

- **Stack:** Python + FastAPI + Psycopg
- **Database:** AWS Postgres — **metadata only** in Milestones 1 and 2, never observations (§1)
- **Public contract:** [`¨pxapi2.json`](docs/pxapi2.json) (OpenAPI 3.0.4), plus one small
  additive read surface, `/providers` (§9a), and a token-protected `/admin` surface (§8)
- **Repo boundary:** this repo is the **public API plus the admin control plane**.
  Harvesting runs elsewhere but coordinated with this API.

---

## 1. Core thesis

End users query every connected statistical source through one interface, one query
grammar and one identifier space, instead of learning each provider's own API.

In Milestone 1 — and, on current plans, Milestone 2 — the router **stores metadata,
not data**:

- **Stored in Postgres:** table metadata records (dimensions, categories/values,
  codelists, source, periods, update timestamps) and per-provider API connection
  and configuration info.
- **Not stored:** observations/values.

When a data request arrives, the router parses it, expands the selection against
stored metadata, translates it into the upstream provider's native query, fetches
live from that provider, and returns the result in the requested output format.

### "Metadata only" is a scope decision, not a permanent constraint

A later milestone is expected to **host observations for tables that have no upstream
API at all** — series a publisher releases only as a pile of CSV/XLSX files, PX files
on a download page, or a statistical annex — which we harvest, normalise and then serve
ourselves through the same PxApi surface. For those tables this service is the origin,
not a router, and there is no upstream to fetch from at request time.

This is deliberately **out of Milestones 1 and 2** and is recorded here only so the
schema and the adapter seam are not designed in a way that forecloses it. Two
consequences are taken now, and nothing more:

- The `table` row carries a **serving mode** (`routed` today, `hosted` later), so a
  hosted table is a value in an existing column rather than a second code path
  retrofitted across the request pipeline.
- Nothing in the metadata model assumes that every table has a live upstream query
  endpoint behind it.

Observation storage itself — physical layout, ingest, versioning, revision history — is
explicitly **not designed here**.

### What this buys, and when

For a provider that already speaks PxApi 2.0 (SCB does), the router adds a hop and
a staleness risk in exchange for very little. The value that justifies this product —
one interface over dissimilar backends, cross-provider search over a unified metadata
store, one identifier space — **only becomes real at provider number two.**

This is stated plainly because it shapes the milestone plan: Milestone 1 is an
internal engineering milestone that proves the skeleton, not a demonstration of the
product.

---

## 2. Architecture

```
                      ┌────────────────────────────────────────────┐
                      │  nordicintel-api          <- THIS REPO     │
  client ─PxApi 2.0──▶│                                            │
                      │  ┌──────────────────────────────────────┐  │
                      │  │ public read surface (PxApi 2.0)      │  │
                      │  │   selection parser + local expansion │  │
                      │  │   maxDataCells pre-flight            │  │
                      │  │   provider data adapter (live fetch) │  │
                      │  └──────────────────────────────────────┘  │
                      │  ┌──────────────────────────────────────┐  │
  operator ──token───▶│  │ admin control plane (bearer token)   │  │
                      │  │   upsert / disable / delete          │  │
                      │  │   providers and tables               │  │
                      │  │   request an out-of-band re-harvest  │  │
                      │  └──────────────────────────────────────┘  │
                      └───────┬───────────────────────────┬────────┘
                              │                           │
                 owns schema + Alembic            live /data fetch
                              │                           │
                  ┌───────────▼──────────┐                │
                  │   AWS Postgres       │                │
                  │   (metadata)         │                │
                  └───────────▲──────────┘                │
                              │ metadata writes           │
   ┌──────────────────────────┴─────────────────┐         │
   │  harvest worker        <- SEPARATE REPO    │         │
   │    imports one harvester package per       │         │
   │    provider, each its own repo             │         │
   │    owns scheduling, cadence, backoff       │         │
   └──────────────────────────▲─────────────────┘         │
                              │                           │
                   ┌──────────┴───────────────────────────▼───────┐
                   │  upstream providers (SCB, …)                 │
                   └─────────────────────────────────────────────-┘
```

### Component boundaries

| Component | Lives in | Responsibility |
|---|---|---|
| **FastAPI service** | **this repo** | The PxApi 2.0 read surface (metadata served purely from Postgres, `/data` translated and forwarded live), the additive `/providers` surface, and the token-protected admin control plane |
| **Postgres metadata store** | schema **owned by this repo** (Alembic) | Provider registry, table registry, alias history, dimensions, categories/values, search index |
| **Harvest worker** | **separate repo** | Runs the recurring refresh across providers. Scheduling, cadence, per-provider rate limiting and failure isolation are **its** concerns, not this repo's |
| **Per-provider harvester packages** | **one repo each** | Provider-specific discovery and metadata extraction behind a shared interface, installed as dependencies of the worker (§8) |

**What this repo does not contain:** the daily harvest run, its scheduler, its
per-provider backoff, or any provider-specific catalogue-walking code. What it *does*
contain is the **live per-request fetch path** to upstream providers, because that sits
on the request path and cannot live anywhere else.

### Adapter contract

Because selection expansion always happens in the shared core, **adapters never parse
PxApi expression syntax**. The adapter interface is:

> Given an explicit enumerated selection, build and execute an upstream request, and
> return normalised observations **carrying a status channel alongside the numeric
> values**.

The status channel is mandatory: JSON-stat's `status` object and PxWeb's `..` (missing)
/ `.` (not applicable) markers must survive translation into a common representation.
Without it, status fidelity is destroyed inside the adapter before any test can observe it.

Adapters must also support **POST-upstream and/or chunk-and-stitch**, because expanded
enumerations can be large (~290 Swedish municipalities, or a full wildcard over a
region dimension) and can exceed upstream GET URL length limits. Chunking carries
correctness obligations: consistent ordering when reassembling, and defined
partial-failure handling if one chunk fails mid-fetch.

> **The adapter interface is formally PROVISIONAL.** See §11.

---

## 3. Identifiers

### Canonical table id

A **stable readable slug**, minted at first harvest and **never recomputed**:

```
scb-tab1267
ssb-07459
```

- Character set restricted to `[a-z0-9._-]` — it is a URL path segment, so `:` and
  `/` are excluded.
- The slug is **not parsed** to select the upstream. Provider selection always comes
  from the registry row.

This costs nothing versus a parsed composite key. Since no observations are stored,
every `/tables/{id}/data` call must already read that table's dimensions, value codes
and provider query template from Postgres in order to translate the query — a registry
lookup is on the hot path regardless.

Keeping provider identity out of the public contract as a parsed component means
moving or mirroring a table between providers later is not a breaking change.

### Alias history

A `table_alias` table maps every historical and provider-native id to the current
table row. An upstream rename **adds an alias** rather than breaking anything.
`GET /tables/{alias}` resolves, and the response body returns the canonical id.

### Retirement

Table rows and their aliases are **never hard-deleted**. A table the harvester no
longer sees is marked `discontinued: true` — it stays resolvable via
`GET /tables/{id}` and is filtered out of `/tables` listings unless
`includeDiscontinued=true`.

**No 404 for a table that once existed.** Saved queries persist the canonical slug.

---

## 4. Data model (Postgres)

Designed **independently, derived from the PxApi 2.0 spec and JSON-stat 2.0 directly.**

Core tables:

| Table | Purpose |
|---|---|
| `provider` | **public:** `id`, `label`, `description`, `website`, `region` (ISO 3166-1 alpha-2) · **internal:** `base_url`, `adapter_type`, `adapter_version`, `rate_limit`, `auth`, `enabled` |
| `table` | `id` (PK, canonical slug), `provider_id` (FK), `native_table_id`, …, `UNIQUE(provider_id, native_table_id)` |
| `table_alias` | `alias` (PK), `table_id` (FK), `kind`, `valid_from`, `valid_to` |
| `dimension` | per-table dimensions, mirroring JSON-stat `id` / `size` / `role` |
| `category` | dimension values — **must carry an `ordinal` column** |

Only the five columns marked *public* on `provider` are ever emitted by `/providers`
(§9a). `base_url`, `auth`, `rate_limit`, `adapter_type` and `adapter_version` are
operational and are never serialised onto a public response.

Hard schema requirements arising from decisions elsewhere in this document:

- **`ordinal` on value/category rows**, preserving order as harvested and mirroring
  JSON-stat `category.index`. Without a defined order over a dimension's values,
  `top(n)` and `bottom(n)` are undefined and the feature has no ground to stand on.
- **`last_harvested_at` on the table row**, so expansion freshness can be disclosed
  via `Dataset.updated` and reasoned about operationally.
- **The provider / table / table_alias registry**, with `UNIQUE(provider_id, native_table_id)`
  and alias validity ranges.
- **A GIN index over a materialised tsvector** spanning label, description, tags,
  variable names and source.
- **The status channel as a first-class concept**, since the differential oracle
  asserts on it.
- **A per-table health/availability state, distinct from `discontinued`**, and an
  **operator-set disable flag distinct from both** (see §7).
- **A `serving_mode` on the table row** (`routed` throughout Milestones 1–2), so hosted
  tables are later a value rather than a parallel code path (see §1).
- **`provider.adapter_type`** naming the harvester entry point that produced the row and
  **`provider.adapter_version`** recording which released version produced it — so a
  metadata defect is traceable to a specific harvester release (see §8).
- **Public provider descriptive columns** (`label`, `description`, `website`, `region`),
  since `/providers` serves them (see §9a).

The schema must support serving `/tables/{id}/metadata` efficiently enough to meet
its latency budget — the JSON-stat projection is worked out from JSON-stat's own
structure (`id`, `size`, `role`, `dimension.*.category.index/label/unit`, `extension`).

### Migrations: Alembic from commit one

**This repo owns the schema.** SQLAlchemy models plus Alembic migrations live here and
are the single definition of the metadata store, from the first commit — not retrofitted
once the tables settle.

The reason is structural rather than stylistic: the schema has **two independent
consumers** (this API and the out-of-repo harvest worker, §8) deployed as **separate
artifacts that will not be released together**. Without migrations there is no shared
statement of what the database currently looks like, and no way to roll a schema change
out ahead of the worker release that needs it. Hand-applied DDL is survivable only while
exactly one process writes, which is precisely the property this design gives up.

Rules:

- **Every schema change ships as a migration.** No `create_all()` outside tests.
- **Exactly one actor runs `alembic upgrade head`** — the API's deploy step, as a
  release/init task. Never the worker, and never at application startup, which would
  race across replicas.
- **Expand → migrate → contract** for anything the worker also touches: add the new
  column, release both sides, then drop the old one. Two deployables mean no schema
  change is atomic.
- **Migrations are reviewed like code**, and downgrades exist for anything applied to a
  database holding a real harvest.
- The **models and migrations are packaged for reuse** by the worker rather than
  duplicated there — see §8b for concrete packaging/deployment options.

---

## 5. Query expansion semantics

**Always expand locally. For every provider, without exception.** No adapter may pass
an expression through, even where the upstream understands it natively.

### Why

Pass-through and local expansion are not two implementations of one behaviour — they
can return **different data for the same request**, invisibly to the caller.
`top(5)` on a time dimension expanded from a harvest up to 24h old gives the five
newest periods *as of the harvest*; passed through to PxWeb it gives the five newest
*as of now*.

If adapter capability decided semantics, the same PxApi query would mean one thing
against one provider and another against the next — and the unified interface, which is
the entire product thesis, would be quietly false. One meaning everywhere is worth more
than freshness on one provider.

### How

1. Every expression (`top(n)`, `bottom(n)`, ranges, `from`/`to`, wildcards) is resolved
   against harvested metadata into an explicit code list **before any upstream call**.
2. The upstream query is therefore always a literal enumeration.
3. `maxDataCells` is an exact pre-flight product of selected category counts — a
   `403` is returned **without ever touching the provider**.

### Fallback

If an expression cannot be resolved from harvested metadata: **fail immediately with
`400`**, naming the offending expression and variable in the spec's own idiom
(e.g. `"Expression top(5) cannot be resolved for variable year"`).

Explicitly rejected:

- **Over-fetch and filter in-process.** It would fetch more cells than the caller was
  permitted to request, directly contradicting the `maxDataCells` pre-flight contract,
  and it makes memory unbounded in a service designed as a translating proxy.
- **Stale-but-disclosed expansion.**
- **On-demand refresh-and-retry** (deferred to Milestone 2).

**Accepted trade-off:** a period published upstream since last night's harvest can
cause an avoidable failure until the next harvest run. Accepted in Milestone 1 in
exchange for full determinism and a request path with no synchronous upstream
metadata calls.

> **Gap to close:** `pxapi2.json` names only `top(5)` as an example and never enumerates
> the expression grammar. The exact supported set must be defined and documented here,
> since the spec does not.

---

## 6. Output formats

**Promised: `json-stat2` and `csv` only.** For every provider.

`px`, `json-px`, `xlsx`, `parquet` and `html` are **not** promised in Milestone 1, and
`/config.dataFormats` honestly advertises exactly the supported subset.

This is deliberate architecture, not laziness. It keeps provider adapters close to
pass-through with light rewriting, and avoids needing a canonical in-memory cube model
that every adapter normalises into and every renderer renders from. It also avoids
synthesizing PX keywords (`CONTENTS`, `MATRIX`, `decimals`, `subject-code`) that may
never have been harvested from a JSON-stat-only upstream.

Formats can be added later.

### Shared capability source (implementation constraint)

`/config.dataFormats` and the `/data` request validator **must read from a single
shared capability source.** If independently hardcoded they will drift, and the API
will advertise one set while rejecting another.

This shared source is deliberately the seam that grows into **per-provider format
capability** when the second provider lands.

---

## 7. Table lifecycle: `discontinued` vs `disabled`

Two distinct conditions that **must not share a field**:

| State | Meaning | Listing behaviour | Id endpoints |
|---|---|---|---|
| `discontinued` | The provider stopped updating it | Hidden unless `includeDiscontinued=true` | Serve normally |
| `disabled` | We could not successfully harvest or serve it | **Excluded entirely** | `503` |
| `operator_disabled` | An operator turned it off through the admin API (§8) | **Excluded entirely** | `503` |

When the harvester (or a retest) hits a table-level failure from the upstream provider,
that table is **recorded with its upstream error and marked disabled** — effectively
dropping support for that specific table until the next harvest or retest succeeds.

- Disabled tables are excluded from `GET /tables` listings entirely. The
  `includeDiscontinued` flag governs `discontinued`, not `disabled`.
- Their id-based endpoints (`GET /tables/{id}`, `/metadata`, `/data`) return an error
  rather than stale or partial content.
- **Self-healing:** a subsequent successful harvest re-enables the table automatically.
  No manual allowlist to maintain.
- The last observed upstream error is stored alongside the table for diagnosis.

**`operator_disabled` is a separate column, not a third value of `disabled`.** This is
a concurrency requirement, not taxonomy: the harvest worker owns `disabled` and the
admin API owns `operator_disabled`, so the two writers never contend for one field and a
successful harvest **cannot silently undo an operator's decision**. A table is serveable
only when neither flag is set. Clearing `operator_disabled` is always an explicit admin
call.

**Error code: `503` with RFC7807.** The condition is temporary, upstream-caused and
self-healing on the next harvest — which is what `503` means. `404` was rejected
because it would contradict the rule that a table which once existed never 404s: the
id and its aliases remain resolvable, and the table is known to us; it is simply not
currently serveable.

> A spec-conformance purist would prefer `404` with an explanatory detail. `503` was
> chosen for semantic honesty over strict adherence to the declared response set, and
> is recorded as deviation 5 in §9.

---

## 8. Harvest worker

**Built multi-provider from day one**, even though only SCB is wired up in Milestone 1.

Required from the start:

- **Per-provider scheduling**
- **Per-provider rate limiting**
- **Per-provider failure isolation** — one provider's outage cannot stall another's refresh

Shaping this correctly now avoids pulling a single scheduled job apart in Milestone 2.

In Milestone 1 the harvest worker is the **single writer** to the metadata tables.
That property is deliberate: it removes any concurrency/serialisation problem until
on-demand refresh arrives.

Responsibilities:

- Discover and refresh the provider's table catalogue (~daily)
- Mint canonical slugs on first sight; never recompute
- Add aliases on upstream rename
- Mark unseen tables `discontinued`
- Mark failing tables `disabled` and record the upstream error
- Maintain `ordinal` ordering and `last_harvested_at`
- Refresh the materialised tsvector

---

## 8a. Admin control plane

A token-protected surface, mounted **in this repo**, alongside the public read surface.
It is the one write path into the metadata store that does not belong to the harvest
worker (§8), and it is what lets an operator act between harvest runs without touching
the database directly.

Concrete endpoints (naming is illustrative, not final):

| Endpoint | Effect |
|---|---|
| `POST /admin/providers` | Register a provider (connection info + public descriptive fields) |
| `PATCH /admin/providers/{id}` | Update connection/config or descriptive fields |
| `POST /admin/providers/{id}/disable` · `/enable` | Operator kill switch for an entire provider |
| `DELETE /admin/providers/{id}` | Remove a provider registration — only sensible with no tables attached, or a defined cascade; **open** |
| `POST /admin/tables/{id}/disable` · `/enable` | Sets/clears `operator_disabled` (§7) — never touches the harvester-owned `disabled` flag |
| `DELETE /admin/tables/{id}` | Given §3's never-hard-delete rule, this is realistically a wrapper around `discontinued: true`, not a row delete — **open** |
| `POST /admin/tables/{id}/reharvest` | Request an out-of-band re-harvest of one table |

All admin routes require bearer token auth **from day one** — unlike the public surface,
where auth is an additive Milestone 2+ layer (§14), the admin surface is a write path
and cannot ship without it.

`reharvest` is a **request, not a guarantee**: the actual harvest still runs inside the
harvest worker (§8), a separate process/deployable. This repo can only record that a
re-harvest was requested; see §8b for concrete ways the two sides can hand that off.

---

## 8b. Packaging & deployment topology

This is genuinely **open** — recorded here as a set of concrete, individually workable
options rather than a decision, since the right choice depends on infrastructure this
document does not own.

**Repo/package shape:**

- **This repo**: public API + admin control plane. Owns the Postgres schema and the
  Alembic migrations (§4).
- **Harvest worker**: a separate repo. Depends on this repo's SQLAlchemy models and
  Alembic migrations as an installed package, so there is one schema definition, not
  two — see the "expand → migrate → contract" rule in §4. Concretely, this means
  publishing that package either to a private PyPI-compatible index (e.g. a private
  package registry, or a simple S3/GCS-backed index), or installing it straight from a
  git ref (`pip install git+https://…@vX.Y.Z`) pinned to a tag. Either is a well-worn
  pattern; a git-ref install is the lower-ceremony starting point.
- **Per-provider harvester packages**: one repo each, each implementing a shared
  interface (the adapter contract, §2) and installed as a dependency of the worker,
  the same way. This is what lets a provider be added without touching the worker's
  own code, only its dependency list.

**Running it — candidate solutions to consider:**

1. **Docker Compose, two service definitions from two images.** One image built from
   this repo (API + admin), one image built from the harvest-worker repo, sharing one
   Postgres (managed, e.g. AWS RDS, not a container in prod). This is the simplest
   thing that is still "two deployables" in the sense §4 requires, and is a reasonable
   default for a project at this stage.
2. **Two independently deployed containers/services**, no compose file coupling them —
   e.g. the API on a small always-on service (ECS/Fargate task, App Runner, a single
   VM) and the worker as a **scheduled job** (ECS scheduled task, a Kubernetes
   `CronJob`, or a plain cron-triggered container) rather than a long-lived process,
   since it only needs to run ~daily (§8). This avoids paying for an always-on worker
   process that is idle nearly all the time.
3. **Kubernetes**, if the rest of the org already runs it: `Deployment` for the API,
   `CronJob` for the scheduled harvest, plus a `Job` template the API's `reharvest`
   admin endpoint can trigger via the Kubernetes API for on-demand re-harvests. This is
   the most machinery, and only worth it if k8s is already the house standard.
4. **A queue instead of a direct DB poke for `reharvest`.** The admin endpoint enqueues
   a message (SQS, or a `reharvest_request` table the worker polls) instead of calling
   the worker directly, so the API never needs network reachability to the worker and
   the worker stays the single writer without a second write path racing it. A plain
   polled table is the lowest-ceremony version of this and needs no new infrastructure.

**Recommendation for now, not a final decision:** option 1 (Compose, two images) to get
running quickly, combined with option 4's `reharvest_request`-table variant of hand-off,
since it needs no message broker and keeps the harvest worker the single writer (§8).
Revisit once there is a second provider and a real deployment target.

---

## 9. Deviations from `pxapi2.json`

`pxapi2.json` declares only `400` / `403` / `404` / `429` as error responses, so any
other status is additive. The register has **five** entries:

| # | Deviation | Reason |
|---|---|---|
| 1 | `totalPages` may be `0` for an empty result set | Spec says `minimum: 1`, while `totalElements` allows `0` — an empty result set is otherwise unrepresentable |
| 2 | ISO 8601 date-time for `updated` | The spec's `pattern` (bare `YYYY-MM-DD`) contradicts its own `format: date-time` |
| 3 | `dataFormats` limited to `json-stat2` + `csv` | See §6 |
| 4 | `501` for out-of-scope endpoints | No `501` appears anywhere in `pxapi2.json` |
| 5 | `503` for disabled tables | See §7 |

Additional spec defects **not** reproduced:

- The `examples` `$ref`s point at `./examplesAsYml/*.yml`, which were not supplied —
  the document does not fully resolve as given. Strip or supply them.
- `/config` carries `tags: Configuration` but no such tag is declared at the top level.
- `Dataset` has `additionalProperties: false` and **requires** `value`, so the
  metadata response must explicitly emit `value: null`.
- No `securitySchemes` are declared anywhere. Auth and our own rate limiting are
  **additive layers** that must not alter the documented request/response shapes.

> Because the spec-conformance sweep was declined for Milestone 1 (§13), this register
> is **documentation-tracked only** and must be maintained by discipline.

### `/config` policy

`/config` reports **our own gateway limits and license posture only** — our
`maxDataCells`, our `maxCallsPerTimeWindow` / `timeWindow`, our license string, our
`defaultLanguage` / `languages`, our `dataFormats`. It does not attempt to summarize
or intersect upstream providers.

Per-provider differences surface **per request** instead: `403` when a selection
exceeds our cell ceiling, `429` when we throttle, `400` when a format or codelist is
unavailable for that table's provider.

*Accepted trade-off:* a client cannot tell from `/config` alone that a given upstream
will refuse something.

Provider attribution lives in `Table.source` and `Dataset.source` (single string),
because the spec offers nowhere else — `SourceReference` is keyed by **language**,
not provider, so it cannot carry per-source citation text.

---

## 9a. `/providers` endpoint

An additive read surface with no PxApi 2.0 counterpart — PxWeb has no cross-provider
concept, so there is nowhere in `pxapi2.json` this could have lived. Same shape reused
for the collection and the singleton:

- `GET /providers` — list, paginated the same way as `/tables`
- `GET /providers/{id}` — singleton; unknown id returns `404`

Deliberately small, end-user-facing fields only, drawn from the *public* columns on
`provider` (§4):

| Field | Source | Notes |
|---|---|---|
| `id` | `provider.id` | |
| `label` | `provider.label` | |
| `description` | `provider.description` | |
| `website` | `provider.website` | |
| `region` | `provider.region` | ISO 3166-1 alpha-2 |
`operator_disabled`; computed at request time, not stored |

**Deliberately excluded:** `base_url`, `auth`, `rate_limit`, `adapter_type`,
`adapter_version` — operational, never serialised (§4) — and anything like uptime or
rate-limit posture, which belongs to `/config` (§9) for our own limits, not a provider
listing. Keeping this endpoint small is the design decision, not an omission.

Unlike tables (§3), a provider row has no alias history — it is not expected to be
renamed post-registration.

---

## 10. Milestone 1 scope

**Milestone 1 is an internal engineering milestone. It is not external-facing.**

It proves the skeleton end-to-end:

```
harvest → metadata store in Postgres → local expression expansion
        → pre-flight maxDataCells check → upstream fetch → json-stat2/csv response
```

No external callers are pointed at it. Interfaces may still move without a deprecation
story.

### Endpoint status

**Fully live** against the full SCB corpus:

| Endpoint |
|---|
| `GET /tables` (search, pagination, `pastDays`, `includeDiscontinued`) |
| `GET /tables/{id}` |
| `GET /tables/{id}/metadata` |
| `GET /tables/{id}/data` |
| `POST /tables/{id}/data` |
| `GET /config` |

**Registered routes returning RFC7807 `501`** (nothing implemented behind them):

| Endpoint |
|---|
| `GET /codelists/{id}` |
| `POST /savedqueries` |
| `GET /savedqueries/{id}` |
| `GET /savedqueries/{id}/data` |
| `GET /savedqueries/{id}/selection` |
| `GET /tables/{id}/defaultselection` |

These are **registered routes**, not absent paths. A registered `501` tells a client
the endpoint is known and planned, keeps the served surface matching the published
contract, and makes Milestone 2 a matter of filling in handlers rather than adding routes.

**Returning `400`, not `501`** (unsupported *values* against a *live* endpoint):

- Any `outputFormat` other than `json-stat2` or `csv`
- Any `codelist=` parameter on `/metadata` or `/data`

`/tables/{id}/data` **is** fully live and `/config.dataFormats` already advertises the
supported set, so a request for `px` is an invalid request against an advertised
capability — exactly what `400` means. Returning `501` there would assert that `/data`
is unbuilt, which is false.

### Search

**Postgres full-text search with a GIN index over a materialised tsvector**, covering
label, description, tags, variable names and source. Not ad-hoc ILIKE.

`GET /tables?query=` is undefined in the spec ("matches a criteria"). Since we own the
metadata database, this is ours to define — and cross-provider metadata search is the
single clearest thing this router does that calling SCB directly cannot. Given
Milestone 1 is otherwise SCB-only, **search is the one part of it that points at the
actual product rather than at the skeleton.**

### Instrumentation

**Minimal timing is in Milestone 1.** Not a tracing stack — one timestamp immediately
before the upstream call and one immediately after. The delta is upstream wait; total
request duration minus that delta is router-attributable overhead. Both recorded per
request.

Two reasons it earns its place beyond the acceptance gate:

1. It is the primary operational signal for a service whose entire job is to translate
   and forward. When a `/data` request is slow the first and almost only question is
   *"us or them"* — a router that cannot answer that must answer it by guesswork in
   every future incident.
2. It feeds the per-provider rate limiting and failure isolation in the harvest worker,
   both of which need real signals about how a provider is behaving.

---

## 11. Only one provider in Milestone 1 — and what that costs

A second provider is **explicitly deferred to Milestone 2**; none is named yet, and
nothing here should be read as committing to a specific one. The consequence is
accepted knowingly: Milestone 1 ships with the adapter interface validated by exactly
one provider that already speaks PxApi 2.0.

### The adapter interface is formally PROVISIONAL

Three commitments are provider-shaped and validated by only the one natively-conforming
provider. Each is **unproven** at the end of Milestone 1:

1. **The status channel.** With SCB alone it carries markers that already match the
   target representation, so it proves no mapping. A provider with genuinely different
   flags (provisional, estimated, confidential/suppressed) will be the first real test.
2. **Local expression expansion over harvested ordinals.** SCB's ordering conventions
   *are* the PxApi ones, so nothing gets normalised.
3. **The unified id space and `table_alias`.** With one provider it never encounters a
   cross-provider collision or a differing native-id shape.

### Not adopted

A **synthetic second adapter** in the test suite (a fake provider with dissimilar code
vocabularies, flags and ordering) was considered and declined. Recorded rather than
hidden: nothing in Milestone 1 exercises the adapter interface against a non-conforming
provider, so the interface's fitness for a second provider remains genuinely untested
until that work begins.

---

## 12. Acceptance gate

### Corpus

The **full SCB PxWeb catalogue**, harvested and queryable. No percentage threshold and
no frozen subset.

**Pass condition:** the full catalogue is harvested; every **enabled** table passes the
three-part differential oracle. Disabled tables are excluded from the pass condition,
but their count and their recorded upstream errors are reported in the gate output — so
a rising disabled count is visible rather than silently absorbed, which is what a plain
failure budget would have hidden.

**Upstream throttling:** on an SCB `429` the runner backs off and continues rather than
aborting. Throttled tables are reported separately from genuinely failing ones, so a
rate limit never masquerades as a correctness failure.

### Correctness oracle

A **differential test** is the blocking gate. For each query, fetch the same selection
from our API and directly from the upstream in the same run, and assert **three things
separately**:

1. **Cell values**, cell-for-cell
2. **Category ordering**
3. **Status / missing-value semantics** — JSON-stat's `status` object versus PxWeb's
   `..` (missing) and `.` (not applicable) markers

Schema-validity against `pxapi2.json` is a **necessary precondition, never sufficient**.
A schema-only gate would pass a router that silently returned Uppsala's figures for
Stockholm, because this system's whole risk surface is translation, not response shape.

### CI sourcing

- **Recorded cassettes on every PR** — speed and determinism, decoupling merges from
  upstream availability and rate limits.
- **Live differential on a schedule and at the release gate** — a genuine oracle where
  it matters.

*Still to define:* cassette staleness detection and refresh policy.

### The reviewer command

A single entry point — `make gate`, wrapping `scripts/gate.py` — with **two explicitly
defined modes**, because the numeric p95 bars are derived from this very run and
therefore cannot exist on first invocation:

```bash
make gate MODE=baseline
```
Runs the full corpus, emits the p95 distribution table per endpoint class, and does
**not** assert on latency. **This is what the reviewer runs first**, and its output is
the source from which the numeric bars are fixed and then committed.

```bash
make gate
```
`MODE=gate` (default). Runs the same corpus and additionally asserts against the bars
in a checked-in thresholds file.

Both modes emit **one durable report file** containing:

1. Harvest coverage of the SCB catalogue (discovered / harvested / enabled / disabled),
   with the disabled list and each one's recorded upstream error
2. Differential oracle results, split into the three assertion classes and reported
   separately
3. Scope conformance: the six live endpoints answer; the six `501` endpoints return
   well-formed RFC7807; unsupported `outputFormat` and `codelist=` values return `400`
4. The p95 table for the four endpoint classes, with end-to-end `/data` wall clock
   reported alongside as an observation
5. Throttled-table count, listed separately from failures

The thresholds file is a **committed, first-class repository artifact**, kept adjacent
to the deviation register. Since the conformance sweep was declined, those two files are
the only mechanical records of the gate's contract — their being reviewable in version
control rather than folklore is the point.

---

## 13. Latency budget

**Shape fixed now; numbers set after the first measurement run.**

| Class | Endpoints | Budget |
|---|---|---|
| Point lookups | `/config`, `/tables/{id}` | One bar — near-constant cost |
| Metadata | `/tables/{id}/metadata` | Own bar — scales with dimension/category cardinality |
| Search | `/tables?query=` | Own bar — the **only** endpoint whose cost scales with catalogue size |
| Data | `/tables/{id}/data` | **Router-attributable overhead only** |

Search is budgeted separately from point lookups deliberately: grouping it with
`/config` would hide exactly the thing that matters.

For `/data`, end-to-end wall clock is recorded and reported as an **observed metric,
never gated**, because it is dominated by SCB. What we own is receipt → upstream call
issued, plus upstream response received → our response emitted.

### Measurement conditions

p95 is measured at **concurrency ≈ 1** (a reviewer running an acceptance script), over
the full SCB corpus, on the acceptance run.

> Because Milestone 1 is an internal engineering milestone,
> these are **latency observations under single-client conditions, not performance
> SLOs.** They must not later be treated as evidence that the same numbers hold under
> concurrent production traffic.

*Still to decide at measurement time:* whether cold-cache / first-request behaviour
counts toward p95.

**Ordering dependency:** the search p95 bar is set from measurement *after* the
GIN/tsvector implementation exists — the search implementation is a prerequisite of
setting its own budget, not a response to it.

### Spec-conformance sweep

**Not in Milestone 1.** A schemathesis-style sweep against `pxapi2.json` with the
deviations in a checked-in file was considered and declined. Recorded as explicitly
out of scope rather than forgotten — the deviations in §9 are consequently tracked in
documentation only, with the acknowledged risk that they can drift unenforced.

---

## 14. Deferred to Milestone 2+

| Item | Note |
|---|---|
| **Second provider adapter** (none named yet) | The first real test of the provisional adapter interface |
| `/codelists/{id}` + `codelist=` | Requires in-flight aggregation for providers lacking server-side aggregation |
| `/savedqueries` (all four operations) | Needs its own abuse bound — the spec declares no auth on the POST |
| `/tables/{id}/defaultselection` + PxWeb params | `defaultSelection`, `savedQuery` on `/metadata` |
| Additional output formats | `px`, `json-px`, `xlsx`, `parquet`, `html` — per provider, where derivable |
| On-demand single-table metadata refresh | Needs its own timeout and circuit breaker so a slow provider cannot stall the API, plus defined serialisation against the harvest job — the harvest worker stops being the single writer |
| Spec-conformance sweep | With deviations as a checked-in allowlist |
| Auth + own rate limiting | Additive layers; must not alter documented request/response shapes |

---

## 16. Open items

- [ ] Define and document the **exact expression grammar** supported (`top(n)`,
      `bottom(n)`, ranges, `from`/`to`, wildcards) — the spec names only `top(5)`
- [ ] Fix the **p95 numbers** from the first `make gate MODE=baseline` run
- [ ] Decide whether **cold-cache / first-request** behaviour counts toward p95
- [ ] Define **cassette staleness detection and refresh policy**
- [ ] Confirm SCB's **live PxApi 2.0 conformance** and catalogue reachability against
      the supplied 2.0 document
- [ ] Decide whether the `501` endpoints appear in the **served** OpenAPI document or
      only in the hand-authored contract
- [ ] Resolve **OpenAPI version**: `pxapi2.json` is 3.0.2 (`nullable: true`, boolean
      `exclusiveMaximum`) while FastAPI emits 3.1 by default — decide which document is
      authoritative for consumers
- [ ] Decide whether the gate report file is **committed** or a build artifact

---

## Appendix — decision summary

| Area | Decision |
|---|---|
| Table id | Stable readable slug + `table_alias` history; provider from registry, never parsed from the id |
| Expansion | Always local, never pass-through; plain `400` when unresolvable; no on-demand refresh |
| Formats | `json-stat2` + `csv` only; unsupported → `400` from a shared capability source |
| `/config` | Our own limits only; upstream differences surface per-request |
| Scope | 6 live endpoints; 6 registered `501`s |
| Oracle | Differential vs SCB: values, category order, status — asserted separately |
| CI | Cassettes on PR, live differential at the gate |
| Search | Postgres GIN + materialised tsvector |
| Failure | Upstream table errors → `disabled`, dropped from listings, `503`, self-healing |
| Latency | Shape fixed now, numbers from the first baseline run |
| Repo | New standalone repo; schema designed from the spec |
| Repo split | This repo: public API + admin control plane. Harvest worker + per-provider harvester packages: separate repos, installed as dependencies (§8, §8a, §8b) |
| `/providers` | Additive endpoint, list + singleton, small end-user-facing field set only (§9a) |
| Deployment | Open; candidate solutions recorded in §8b, not decided |
| Milestone 1 | Internal engineering milestone, SCB-only, adapter interface provisional |
