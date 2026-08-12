# AGENT.md — How to navigate and maintain this project's documentation

This file is for any agent working on `archlens`. It points to the right
documents and summarizes the documentation hygiene expected in this repository.

## Document map

| Document | Content | When to read it |
|---|---|---|
| [`README.md`](README.md) | Entry point: positioning, installation, quickstart, MCP setup | First stop for onboarding or user-facing repo updates |
| [`docs/PRD.md`](docs/PRD.md) | Problem, vision, personas, scope, success metrics | When you need product intent and scope boundaries |
| [`docs/SPEC-FONC.md`](docs/SPEC-FONC.md) | Observable behavior: CLI commands, flags, error messages, MCP tools, skill workflows | Before changing anything a user or agent sees |
| [`docs/SPEC-TECH.md`](docs/SPEC-TECH.md) | Modules, data model, SQLite schema, algorithms, JSON contract | Before changing internal architecture in `src/archlens/` |
| [`docs/ADR.md`](docs/ADR.md) | Architecture decisions: context, choice, consequences | Before revisiting an existing technical choice |
| [`../ccc-radar-skill/`](../ccc-radar-skill/) | Companion skill repo: agent workflow, bundled rule packs, operational guidance | Whenever an ArchLens change can affect the skill or its docs |

`README.md` stays intentionally short. The specifications and ADRs hold the
authoritative detail.

## Documentation maintenance rules

1. Write and update user-facing documentation in English, including
   `README.md`, `docs/`, generated-document templates, and report indexes.
2. Keep `README.md` focused on onboarding and day-to-day usage.
3. Update `docs/SPEC-FONC.md` in the same change as any CLI, MCP, or skill
   behavior change.
4. Update `docs/SPEC-TECH.md` in the same change as any architectural or data
   model change.
5. Record durable design decisions in `docs/ADR.md` instead of leaving them only
   in commit messages.
6. If a change in ArchLens affects the companion skill, update
   `../ccc-radar-skill/` in the same pass.
7. Keep changes consistent with the existing codebase and ensure the code is
   correct, including its behavior, contracts, and edge cases.

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

## Hugging Face model downloads

When a model must be downloaded with `hf`, **disable `SSL_CERT_FILE` first** in
the current shell, otherwise environments with proxy/intercepted TLS often fail
with `CERTIFICATE_VERIFY_FAILED`.

Reference command for the repository's default model:

```bash
env -u SSL_CERT_FILE uvx --from huggingface_hub hf download \
  jinaai/jina-code-embeddings-1.5b \
  --local-dir ~/models/jina-code-embeddings-1.5b
```

The local path expected by default on the `archlens` side is
`~/models/jina-code-embeddings-1.5b`.
