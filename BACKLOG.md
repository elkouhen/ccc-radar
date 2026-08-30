# SystemLens backlog

The active delivery backlog is maintained in the GitHub Project
[SystemLens Backlog](https://github.com/users/elkouhen/projects/3).

Each active item is a repository issue. Its issue body is the source of truth
for the problem, implementation direction, dependencies, and acceptance
criteria. Create and prioritize new work in that project rather than adding a
second Markdown backlog here.

## Migrated active items

| Backlog ID | GitHub issue |
|---|---|
| SL-006 | [#1 — Restore a reliable browser acceptance gate](https://github.com/elkouhen/systemlens/issues/1) |
| SL-007 | [#2 — Add a shareable HTML export without local deep links](https://github.com/elkouhen/systemlens/issues/2) |
| SL-011 | [#5 — Extract Java/Spring S3 integration evidence](https://github.com/elkouhen/systemlens/issues/5) |
| SL-015 | [#6 — Resolve prefixed and suffixed Kubernetes workload names safely](https://github.com/elkouhen/systemlens/issues/6) |
| SL-012 | [#7 — Add source-specific Kafka, MongoDB, and S3 runtime adapters](https://github.com/elkouhen/systemlens/issues/7) |
| SL-013 | [#8 — Add explicit Kubernetes capacity context to runtime hotspots](https://github.com/elkouhen/systemlens/issues/8) |
| SL-014 | [#9 — Compare bounded runtime windows for regressions](https://github.com/elkouhen/systemlens/issues/9) |
| SL-016 | [#10 — Separate framework endpoints (actuator, swagger) from business APIs in module summaries](https://github.com/elkouhen/systemlens/issues/10) |

The completed SL-001 to SL-005, SL-008, and SL-017 records remain available in
Git history and in their closed GitHub issues. They are not active backlog
items.

## Improvement-loop findings (2026-08-30)

The only available requested example, `sample-spring-kafka-microservices`,
confirmed and closed two P1 gaps during this run: KafkaTemplate producers whose
receiver is named `template` rather than `kafkaTemplate`, and chained Kafka
Streams `.to(...)` calls omitted from module-level usage. Both are implemented
with regression tests in the current working tree.

The remaining audit gap is environmental: three requested example directories
are absent from `~/examples`. No new active backlog item is warranted until
those repositories are available or another reproducible gap is found.
