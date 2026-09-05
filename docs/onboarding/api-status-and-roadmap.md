# NordicIntel API status and roadmap

## Purpose

This note summarizes what `nordicintel-api` appears to contain today, how it relates to the sibling NordicIntel repositories, what looks implemented versus still planned, and which preconditions should probably come next before an API implementation can be called underway.

## Evidence reviewed

### API repo files

- `README.md`
- `AGENTS.md`
- `docs/pxapi2.yml`
- `docs/pxapi2.json`
- `testing/harvestor-new-plan.md`
- `testing/nordicintel-core-src.md`
- `testing/sv_labels.json`
- The repository root layout as currently checked out

### Cross-repo seam files inspected

These are not part of this repository, but they explain where the future API is expected to plug in.

- In `nordicintel-core`: `README.md`, `src/nordicintel_core/models/adapters.py`
- In `nordicintel-harvest`: `README.md`, `src/nordicintel_harvest/registry.py`, `src/nordicintel_harvest/worker.py`
- In `nordicintel-adapter-pxweb2`: `README.md`, `pyproject.toml`, `src/nordicintel_adapter_pxweb2/adapter.py`, `tests/test_adapter.py`

## What this repo contains today

### Facts

- `README.md` currently contains only a minimal title and one-line description: “Public and Admin API for NordicIntel”.
- `AGENTS.md` points contributors to issue-tracker and domain-documentation conventions, but it does not define an implementation architecture for an API service.
- `docs/pxapi2.yml` contains a substantial OpenAPI description with endpoints such as:
  - `/tables`
  - `/tables/{id}`
  - `/tables/{id}/metadata`
  - `/tables/{id}/defaultselection`
  - `/tables/{id}/data`
  - `/codelists/{id}`
  - `/config`
  - `/savedqueries`, `/savedqueries/{id}`, `/savedqueries/{id}/data`, `/savedqueries/{id}/selection`
- `docs/pxapi2.yml` also defines substantial schema material for JSON-stat datasets, links, codelists, saved queries, and problem responses.
- `docs/pxapi2.json` is a second OpenAPI artifact with overlapping subject matter.
- `testing/harvestor-new-plan.md` is a long planning note that explicitly says the next repo to build was `nordicintel-harvest`, and that the API should be built afterward against the same core interfaces.
- `testing/nordicintel-core-src.md` is a repacked source export of `nordicintel-core`, used as analysis input rather than as runnable application code.
- `testing/sv_labels.json` is sample data containing Swedish table labels and IDs.
- A very large `testing/scb_metadata.json` artifact exists in the repo, but it was too large to fully read through the editor sync path during this review.
- At the time of inspection, the repository root contained documentation and testing/planning assets, but no `pyproject.toml`, no `src/` tree, and no obvious runnable application package.

### Inference

- This repository currently behaves more like a specification-and-planning workspace than like an already-started API service codebase.

## How this repo relates to the other NordicIntel repos

### Facts

- `testing/harvestor-new-plan.md` says the intended order is:
  1. use `nordicintel-core` as the shared contract/infrastructure layer
  2. build `nordicintel-harvest` to populate and maintain the catalogue
  3. build the API afterward on the same core interfaces
- `nordicintel-core/README.md` says core owns shared contracts and infrastructure, including metadata composition, stable canonical table IDs, JSON-stat handling, and optional DB/HTTP/migration integrations.
- `nordicintel-harvest/README.md` says harvest owns the scheduler, worker, and bootstrap/operator commands, while core owns schema, repositories, adapter protocol, and HTTP transport.
- `nordicintel-adapter-pxweb2/README.md` and source files show that provider-family adapters are packaged separately and loaded into harvest through the `nordicintel.adapters` entry-point group.
- `testing/harvestor-new-plan.md` section 11 outlines a future API layer that should map onto core repositories for provider management, schedules, queue operations, metadata reads, and live routed data fetches.

### Inference

- The sibling repos are already carrying most of the executable groundwork. The API repo currently appears to be the place where the public/admin surface is being designed before a service package is created.

## What appears implemented versus planned

### Implemented or at least concretely specified

#### Facts

- The repository has a meaningful OpenAPI description in `docs/pxapi2.yml`.
- The repository also has a JSON-form spec variant in `docs/pxapi2.json`.
- The planning material in `testing/harvestor-new-plan.md` goes beyond vague ideas; it spells out how future API endpoints could map to existing core repository methods.
- Sample payload-like artifacts exist under `testing/`, which suggests the spec work is informed by real or realistic provider data.

### Planned / not evidenced as implemented in this repo

#### Facts

No inspected file showed evidence of:

- an application package under `src/`
- a Python project manifest such as `pyproject.toml`
- API framework wiring
- auth middleware
- DB session construction
- request handlers
- route tests
- integration tests
- packaging or deployment files for a service

#### Inference

- The OpenAPI design is ahead of the runnable implementation.
- If code exists for the API, it is not in the inspected working tree for this repository.

## Spec quality observations

### Facts

- `docs/pxapi2.yml` identifies itself as `openapi: "3.0.2"`, with `info.title: PxApi` and `info.version: "2.0"`.
- `docs/pxapi2.json` identifies itself as `openapi: "3.0.4"`, with `info.title: PxWebApi` and `info.version: "v2"`.
- `docs/pxapi2.json` also declares a `servers` entry with `"/api/v2"` near the top of the document.

### Inference / caution

- The YAML and JSON specs may not currently be generated from a single source of truth.
- At minimum, their top-level metadata has diverged; further divergence is possible, but this review did not line-by-line diff the full artifacts.

## What the planning document says should happen next

### Facts from `testing/harvestor-new-plan.md`

The plan argues that the API should come after working harvest/adapter integration, and it calls out several concrete concerns:

- core already owns schema, repositories, JSON-stat storage, search, and job/schedule persistence
- harvest should validate metadata ingestion before an API is built on top of it
- the future API should use request-scoped core DB sessions rather than redefining schema logic
- initial admin endpoints should likely wrap existing core repository operations such as provider management, schedule management, job enqueue/detail/history, item listing, queue summary, and cancellation
- live data should route through adapters via `fetch_data(...)`

The same planning document also lists core follow-ups that it considers helpful or necessary before smooth integration, including:

- native-ID lookup support
- cancellation-safe finalization behavior
- authoritative reappearance handling for retired tables
- clarified provider-disable semantics

### Inference

- Even if API code started tomorrow, the best first slice would probably be an admin/control plane over already-existing core and harvest behaviors, not a greenfield redesign of metadata and queue semantics.

## Practical preconditions before implementation work

This section mixes facts from the inspected material with explicit recommendations.

### Documentation preconditions

#### Facts

- There is currently no inspected onboarding or architecture note in this repo that explains how `nordicintel-api` should be scaffolded.
- `testing/harvestor-new-plan.md` is rich in implementation guidance, but it lives under `testing/` and reads like a review artifact rather than a stable top-level architecture note.

#### Recommendations / inferences

- Promote the API-relevant parts of `testing/harvestor-new-plan.md` into a stable architecture document once the intended service shape is agreed.
- Choose one OpenAPI source of truth and make the other artifact generated from it.
- Add an explicit repo README section describing whether this repo is still planning-only or whether implementation has started.

### Implementation preconditions

#### Facts

- `nordicintel-core` and `nordicintel-harvest` already define the seams the API is supposed to use.
- `nordicintel-adapter-pxweb2` already implements a real adapter package against those seams.

#### Recommendations / inferences

Before adding endpoints, the next concrete steps likely are:

1. create an actual service scaffold (`pyproject.toml`, `src/`, tests, lint/type-check config)
2. codify which framework will host the public/admin API
3. lock down the OpenAPI source-of-truth workflow
4. confirm the core integration gaps called out in `testing/harvestor-new-plan.md`
5. ensure harvest/bootstrap can populate a real catalogue so API work is exercised against real metadata, not only against examples
6. decide which endpoints are phase 1 admin endpoints versus later public/live-data endpoints

## Known caveats from this review

### Facts

- `testing/scb_metadata.json` could not be fully opened through the editor sync path because of its size.
- The presence or absence of files was assessed from the current working tree only.
- This was a documentation/source inspection, not an execution test of an API service.

### Inference

- Some implementation work could exist in another branch or outside this checkout, but there is no evidence of it in the inspected repository state.

## Bottom line

Today, `nordicintel-api` looks like a design/specification repository rather than a runnable API service repository.

What is solid already:

- a broad OpenAPI surface in `docs/pxapi2.yml`
- a parallel JSON OpenAPI artifact in `docs/pxapi2.json`
- a detailed implementation-order and integration plan in `testing/harvestor-new-plan.md`
- strong sibling foundations in `nordicintel-core`, `nordicintel-harvest`, and `nordicintel-adapter-pxweb2`

What still looks pending in this repo:

- project scaffolding
- service implementation
- tests
- operational docs specific to the API runtime
- a resolved source-of-truth story for the spec artifacts

So the roadmap is less “invent the API from scratch” and more “turn the existing spec and cross-repo plan into an actual service, after harvest-backed metadata flow is real and the shared core seams are confirmed.”
