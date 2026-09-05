# nordicintel-api

Documentation, API contract, and planning workspace for the future NordicIntel public and admin API.

> **Current status**
> 
> This repository appears **spec- and planning-heavy today**. In the current checkout, it contains OpenAPI artifacts, onboarding notes, and test/planning material, but **no `src/` tree, no `pyproject.toml`, and no runnable API service package**.

## What this repo is today

The most important artifacts in this checkout are:

- `docs/implementation-status.md` — a newcomer-friendly summary of what looks implemented here versus still planned.
- `docs/onboarding/api-status-and-roadmap.md` — the existing status/roadmap note based on a repo inspection.
- `docs/pxapi2.yml` — a substantial OpenAPI YAML artifact for the intended API surface.
- `docs/pxapi2.json` — a JSON-form OpenAPI artifact covering the same general API area.
- `testing/harvestor-new-plan.md` — a cross-repo implementation note that argues `nordicintel-harvest` should be built before this API.
- `testing/` sample and analysis artifacts — working material that appears to support design and planning.

## What depends on the other repos

This repository does not currently stand alone. The executable foundations appear to live in the sibling repos:

- `../nordicintel-core` — shared contracts and infrastructure, including JSON-stat handling, database repositories, and migration tooling.
- `../nordicintel-harvest` — the scheduler, worker, and bootstrap/operator commands that populate and maintain the catalogue.
- `../nordicintel-adapter-pxweb2` — a PxAPI v2 adapter package intended to plug into harvest.

Taken together, those repos look like the implementation base that a future API service in this repo would sit on top of, rather than duplicate. The API repo currently looks more like the place where the surface is being specified and explained.

## OpenAPI artifact status

The OpenAPI story needs to be read carefully rather than heroically:

- `docs/pxapi2.yml` declares `openapi: "3.0.2"`, `title: PxApi`, and `version: "2.0"`.
- `docs/pxapi2.json` declares `openapi: "3.0.4"`, `title: "PxWebApi"`, and `version: "v2"`.
- `docs/pxapi2.json` also includes a `servers` entry for `"/api/v2"`.
- Both artifacts cover overlapping endpoint groups such as `/tables`, `/config`, `/savedqueries`, and `/codelists/{id}`.

What I did **not** verify is that the YAML and JSON files are byte-for-byte equivalent or generated from one source of truth. Their top-level metadata already differs, so they should currently be treated as **related artifacts**, not as confirmed synchronized outputs.

One additional caveat: `docs/pxapi2.yml` contains references to example files under `docs/examplesAsYml/`, but that directory is not present in this checkout.

## Present facts vs. next-step recommendations

### Present facts

- The current repository root contains documentation and planning assets, not a runnable API implementation.
- The main API-facing artifacts today are the OpenAPI files and the onboarding/planning notes under `docs/` and `testing/`.
- The sibling repos already carry executable work that this future API is expected to depend on.

### Next-step recommendations

These are recommendations, not claims about the current state:

- Pick one OpenAPI source of truth and generate the other artifact from it.
- Promote any durable architecture guidance out of `testing/harvestor-new-plan.md` into a stable document under `docs/` when the service shape is settled.
- Start the first implementation slice against already-existing `../nordicintel-core` and `../nordicintel-harvest` seams instead of redefining metadata, queue, or migration behavior inside this repo.

## Where to read next

- `docs/implementation-status.md`
- `docs/onboarding/api-status-and-roadmap.md`
- `testing/harvestor-new-plan.md`
- `../nordicintel-core/README.md`
- `../nordicintel-harvest/README.md`
- `../nordicintel-adapter-pxweb2/README.md`
