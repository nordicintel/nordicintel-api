# Implementation status

This document is a newcomer-oriented snapshot of what `nordicintel-api` appears to contain **in the current checkout**. It separates present facts from recommendations so the repo status is clear without guesswork.

## Current state of this repository

### Present facts

- The repository root currently contains `README.md`, `AGENTS.md`, `docs/`, `testing/`, and `testing.ipynb`.
- In this checkout there is **no `src/` tree**, **no `pyproject.toml`**, and **no `*.py` implementation files** in the repository.
- `README.md` was previously only a title and one-line description.
- `AGENTS.md` gives contributor/process pointers, especially to issue tracking and domain-documentation conventions.
- The main API artifacts in this repo today are `docs/pxapi2.yml`, `docs/pxapi2.json`, `docs/onboarding/api-status-and-roadmap.md`, and `testing/harvestor-new-plan.md`.
- `testing/` also contains working artifacts such as `testing/scb_metadata.json`, `testing/scb_metadata.jsonl`, and `testing/sv_labels.json`.

### What that suggests

Based on the files present, this repository appears **spec- and planning-heavy today**, not like an already-started runnable API service.

That conclusion is based on the current working tree only. If API code exists elsewhere, it is not evidenced in this checkout.

## What looks implemented here versus still planned

### Present facts: implemented or concretely documented here

- `docs/pxapi2.yml` contains a substantial OpenAPI description with routes including `/tables`, `/tables/{id}`, `/tables/{id}/metadata`, `/tables/{id}/defaultselection`, `/tables/{id}/data`, `/config`, `/savedqueries`, and `/codelists/{id}`.
- `docs/pxapi2.json` is a second OpenAPI artifact covering the same general API area.
- `docs/onboarding/api-status-and-roadmap.md` already documents the repo state and cross-repo dependencies in narrative form.
- `testing/harvestor-new-plan.md` is a detailed implementation-order note connecting future API work to the core and harvest seams.
- The sample data and analysis files under `testing/` suggest the spec work is informed by realistic provider material rather than pure placeholder text.

### Present facts: not evidenced as implemented here

No inspected file in this repository showed evidence of:

- an application package under `src/`
- a Python project manifest such as `pyproject.toml`
- framework wiring for an API service
- request handlers or route modules
- auth middleware
- DB session setup for a service runtime
- route tests or integration tests for this repo
- deployment/runtime files for an API process

### What that suggests

The OpenAPI design is materially ahead of the runnable implementation in this repository.

## How this repo depends on the sibling repos

### Present facts

The sibling READMEs describe the implementation base this repo appears to depend on:

- `../nordicintel-core/README.md` says core owns shared contracts and infrastructure, including JSON-stat handling, metadata models, repositories, and migrations.
- `../nordicintel-harvest/README.md` says harvest owns the scheduler, worker, and bootstrap/operator processes that keep the catalogue current.
- `../nordicintel-adapter-pxweb2/README.md` says the adapter package implements the PxAPI v2 harvesting adapter and plugs into harvest through the adapter entry-point mechanism.
- `testing/harvestor-new-plan.md` explicitly recommends building `nordicintel-harvest` before this API, then building the API against the same core interfaces.

### What that suggests

This repo currently looks intended to sit on top of `../nordicintel-core`, `../nordicintel-harvest`, and adapter packages, rather than re-implement their schema, queue, transport, or adapter responsibilities.

## OpenAPI artifact situation

### Present facts

- `docs/pxapi2.yml` declares `openapi: "3.0.2"`, `title: PxApi`, and `version: "2.0"`.
- `docs/pxapi2.json` declares `openapi: "3.0.4"`, `title: "PxWebApi"`, and `version: "v2"`.
- `docs/pxapi2.json` includes a `servers` entry for `"/api/v2"`.
- Both artifacts cover overlapping endpoint groups such as `/tables`, `/config`, `/savedqueries`, and `/codelists/{id}`.
- `docs/pxapi2.yml` references example files under `docs/examplesAsYml/`, but that directory is not present in this checkout.

### Caution

The YAML and JSON files should not currently be described as guaranteed synchronized outputs unless that is verified elsewhere. Their top-level metadata differs, and this review did not perform a full line-by-line equivalence check.

## Recommendations for the next documentation/implementation step

This section is intentionally about recommendations, not present facts.

1. Choose one OpenAPI source of truth and generate the other artifact from it.
2. Keep `README.md` explicit that this repo is currently documentation/spec-heavy.
3. Move durable API architecture guidance out of `testing/harvestor-new-plan.md` into a stable `docs/` note once the intended service slice is agreed.
4. Start implementation against the seams already described in `../nordicintel-core` and `../nordicintel-harvest`, rather than redefining those layers in this repo.

## Where to read next

- `README.md`
- `docs/onboarding/api-status-and-roadmap.md`
- `testing/harvestor-new-plan.md`
- `../nordicintel-core/README.md`
- `../nordicintel-harvest/README.md`
- `../nordicintel-adapter-pxweb2/README.md`