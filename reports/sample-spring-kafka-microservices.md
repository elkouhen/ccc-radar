# Inventory improvement audit — `sample-spring-kafka-microservices`

## Scope and preflight

Only this target existed at `/Users/elkouhen/examples` on 2026-08-30. The
other requested paths (`microservices-kafka-mq`,
`booking-microservices-java-spring-boot`, and
`fully-completed-microservices-Java-Springboot`) were absent and could not be
audited. No source or build file in the example was changed. Its pre-existing
working tree contained untracked `.systemlens/` and `architecture.html`; only
`.systemlens/` was removed and recreated as authorized by `improve.md`.

| Item | Result |
|---|---|
| Branch / commit | `master` / `43bb58b` |
| Active configuration | include `**/*`; excludes `.git/**`, `.venv/**`, `node_modules/**`, `.systemlens/**` |
| SystemLens | `0.1.0` from the local project environment |
| AST prerequisite | OK; Tree-sitter Java available |
| Semgrep | Not installed; current AST indexer does not require it |
| Final index | Schema compatible; 37 files scanned; 13 endpoints; 47 materialized relations |
| Doctor | All checks OK; no extraction diagnostics |
| Raw JSON | [`reports/raw/sample-spring-kafka-microservices-current/`](raw/sample-spring-kafka-microservices-current/) |

## Normalized inventories

### Services and HTTP

Three production services were found in both analyses: `order-service`,
`payment-service`, and `stock-service`. Direct code inspection found three
served routes in `OrderController.java`: `POST /orders` (line 37),
`POST /orders/generate` (45), and `GET /orders` (51). SystemLens reports the
same three routes, all owned by `order-service`; no HTTP client or HTTP edge is
present. Test sources were excluded from the direct scan.

### Kafka

The independent production scan found 10 usages:

| Role | Topic | Framework | Evidence |
|---|---|---|---|
| consume | `orders` | Spring Kafka | `payment/PaymentApp.java:30`, `stock/StockApp.java:29` |
| produce | `orders` | Spring Kafka | `order/OrderController.java:40`, `order/OrderGeneratorService.java:32` |
| produce | `payment-orders` | Spring Kafka | `payment/OrderManageService.java:36` |
| produce | `stock-orders` | Spring Kafka | `stock/OrderManageService.java:36` |
| consume | `payment-orders`, `stock-orders`, `orders` | Kafka Streams | `order/OrderApp.java:71-72`, `75`, `90-91` (`builder.stream`) |
| produce | `orders` | Kafka Streams | `order/OrderApp.java:80` (`.to`) |

The final SystemLens endpoint inventory contains the same 10 Kafka facts,
with resolved `Order` payloads. Its method-level inventory contains 10 facts:
`order-service` 6, `payment-service` 2, `stock-service` 2. The producers and
consumers form resolved topic relations for all three topics. The `orders`
read used to be reported as an orphan because it is a local Kafka Streams
table input; it is retained as a valid consumer and is not turned into an
inter-service edge.

### MongoDB

Neither direct inspection nor SystemLens found `@Document`,
`MongoRepository`, `ReactiveMongoRepository`, `MongoTemplate`, or a Mongo
collection. The repositories in Payment and Stock are JPA repositories, so
their `findById` and `save` calls are not Mongo facts.

### Edges and graph

The graph contains service and topic nodes for the three services and
`orders`, `payment-orders`, and `stock-orders`. HTTP edges: 0. Kafka topic
flows: `order-service -> orders -> payment-service`,
`order-service -> orders -> stock-service`,
`payment-service -> payment-orders -> order-service`, and
`stock-service -> stock-orders -> order-service`; same-service stream/table
consumption is not presented as an inter-service dependency. Evidence paths
are relative to the example root in persisted output.

SystemLens quality score: **5/5** for the requested Java/Spring HTTP/Kafka
scope. Services, HTTP routes, Kafka usages, resolved topics/payloads, Mongo
absence, and compatible cross-service topic flows are covered. The score does
not count same-service Kafka Streams table reads as missing edges.

## Confirmed improvements

Two reproducible P1 false negatives were fixed in SystemLens:

1. `KafkaTemplate.send` now resolves receivers declared as fields or
   parameters with generic names such as `template`, while remaining
   conservative about unrelated `send` calls.
2. Chained Kafka Streams calls such as `join(...).to("orders")` are included
   in module-level method usage; complex AST receivers no longer hide the
   operation.

Regression coverage is in `tests/test_ast_only.py` and
`tests/test_modules.py`. Focused validation: 59 tests passed; Ruff passed.

## Out of scope and limits

No gRPC, RabbitMQ/AMQP, JMS, or proprietary messaging protocol was observed.
The repository does not expose HTTP clients. The three absent example
repositories require an external workspace change before their audits can be
completed. PNG rendering could not be regenerated because no Draw.io or image
conversion binary is installed locally; the Draw.io XML sources and the
SystemLens HTML export are provided.

## Diagram sources

- [`sample-spring-kafka-microservices-systemlens.drawio`](assets/sample-spring-kafka-microservices-systemlens.drawio)
- [`sample-spring-kafka-microservices-direct.drawio`](assets/sample-spring-kafka-microservices-direct.drawio)
- [`sample-spring-kafka-microservices-systemlens.html`](assets/sample-spring-kafka-microservices-systemlens.html)
