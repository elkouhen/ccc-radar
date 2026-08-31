# AGENTS.md — How to navigate and maintain this project's documentation

This file is for any agent working on `systemlens`. It points to the right
documents and summarizes the documentation hygiene expected in this repository.

## Document map

| Document | Content | When to read it |
|---|---|---|
| [`README.md`](README.md) | Entry point: positioning, installation, quickstart, MCP setup | First stop for onboarding or user-facing repo updates |
| [`docs/PRD.md`](docs/PRD.md) | Problem, vision, personas, scope, success metrics | When you need product intent and scope boundaries |
| [`docs/SPEC-FONC.md`](docs/SPEC-FONC.md) | Observable behavior: CLI commands, flags, error messages, MCP tools, skill workflows | Before changing anything a user or agent sees |
| [`docs/SPEC-TECH.md`](docs/SPEC-TECH.md) | Modules, data model, SQLite schema, algorithms, JSON contract | Before changing internal architecture in `src/systemlens/` |
| [`docs/ADR.md`](docs/ADR.md) | Architecture decisions: context, choice, consequences | Before revisiting an existing technical choice |
| [`BACKLOG.md`](BACKLOG.md) | Prioritized, acceptance-tested architecture and delivery work | When planning or implementing deferred improvements |
| [`../systemlens-skill/`](../systemlens-skill/) | Companion skill repo: agent workflow, bundled rule packs, operational guidance | Whenever an SystemLens change can affect the skill or its docs |
| [`../systemlens-observability-lab/`](../systemlens-observability-lab/) | Infrastructure and a test application for observability/integration testing | When validating SystemLens against a real running app/infra, or working on observability-related integration tests |

`../systemlens-skill/` and `../systemlens-observability-lab/` are separate
repositories (`https://github.com/elkouhen/systemlens-skill` and
`https://github.com/elkouhen/systemlens-observability-lab`), expected as
siblings of this repository in a multi-repo workspace. If a directory is
absent from the current checkout, skip the rules that reference it rather
than treating it as a broken link.

`README.md` stays intentionally short. The specifications and ADRs hold the
authoritative detail.

## Source-of-truth and conflict resolution

Apply requirements in this order: explicit user and platform safety
requirements, accepted ADRs, functional and technical specifications, tests,
then the current implementation. The PRD provides product intent when those
sources leave a product decision open.

Do not silently choose when a specification, test, and implementation disagree.
Report the conflict, preserve the public contract unless the requested change
explicitly alters it, and update the affected source of truth and regression
test together.

## Documentation maintenance rules

1. Write and update user-facing documentation in English, including
   `README.md`, `docs/`, generated-document templates, and report indexes.
2. Keep `README.md` focused on onboarding and day-to-day usage.
3. Update `docs/SPEC-FONC.md` in the same change as any CLI, MCP, or skill
   behavior change.
4. Update `docs/SPEC-TECH.md` in the same change as any architectural or data
   model change.
5. Update the authoritative documentation in the same pass whenever an
   algorithm changes, including its invariants, fallback behavior and
   complexity or performance implications when relevant.
6. Record durable design decisions in `docs/ADR.md` instead of leaving them only
   in commit messages.
7. If a change in SystemLens affects the companion skill, update
   `../systemlens-skill/` in the same pass.
8. Keep changes consistent with the existing codebase and ensure the code is
   correct, including its behavior, contracts, and edge cases.

## Non-negotiable engineering invariants

1. Persist source evidence only as paths relative to the indexed project root.
   Resolve local paths only at export time through `--root-path`; never persist
   WSL or machine-specific source roots.
2. Preserve compatibility with existing SQLite indexes. Schema migrations must
   be additive where possible and occur before the index transaction.
3. Exports consume the persisted architecture snapshot; they must not silently
   re-parse source files or turn unresolved dynamic facts into guessed static
   dependencies.
4. Keep extraction conservative: retain ambiguity and confidence in persisted
   facts instead of inventing a target or relationship.
5. Never place credentials, tokens, or unredacted secret values in persisted
   facts, generated exports, fixtures, or documentation.

## Validation

Use the project environment and commands below as the canonical validation
path. Set up the development environment first when needed:

```bash
uv sync --group dev
```

For Python changes, run focused tests and Ruff:

```bash
uv run ruff check src tests
uv run pytest tests/path/to_relevant_test.py
```

Run the full default suite for public contracts, storage migrations, extractor
changes, or before a release:

```bash
uv run mypy
uv run pytest
```

When changing generated HTML interactions, also run the browser integration
test when Chrome is available:

```bash
SYSTEMLENS_CHROME_BIN=/path/to/chrome uv run pytest -m slow tests/test_browser_export.py
```

If the prescribed environment or command is unavailable, do not substitute an
unverified setup silently: report the missing prerequisite and the validation
that could not be performed.

## Cross-functional review

Before delivering a non-trivial change, examine it from the relevant points of
view and state any material trade-off or unverified risk:

1. **Product:** does it serve a concrete user task and preserve the intended
   scope and terminology?
2. **UX and accessibility:** is the primary path discoverable, progressively
   disclosed, usable at constrained viewport sizes, and operable by keyboard?
3. **Development and QA:** are public contracts preserved, edge cases covered,
   and behavior verified beyond string-level output when interaction is
   involved?
4. **Architecture and data quality:** are static facts, confidence levels,
   ambiguity handling, and compatibility with existing indexes explicit?
5. **Operations and security:** are offline/network dependencies, performance,
   local-path disclosure, and deployment consequences understood?

Apply only the perspectives relevant to the change. Do not use this checklist
as a substitute for proportionate implementation and tests.

## Deferred HTML accessibility and delivery concerns

Until explicitly reprioritized by the user, do not propose, implement, or
block work on the generated HTML export's CDN dependencies, ARIA semantics, or
keyboard navigation. These concerns are intentionally deferred and must not
expand the scope of an otherwise requested UX or graph-rendering change.
