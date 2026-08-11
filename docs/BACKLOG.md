# Product backlog

This backlog records proposed product changes before implementation. An item is
implemented only after its behaviour and acceptance criteria have been agreed.

## UX-001 — Restore route discovery from Explore

**Status:** Implemented  
**Priority:** High

### Problem

The Explore view currently supports searching for a microservice, topic, or
collection. Route discovery between services is hidden in advanced controls,
so a user cannot naturally enter a route query from their primary exploration
flow.

### Intended outcome

From Explore, a user can deliberately search either for one architecture
resource or for an itinerary between services, without needing to discover an
advanced control first.

### Agreed interaction

Use one search field. A query containing `->` is interpreted as an itinerary;
any other query searches an architecture resource. The placeholder and inline
help must show both forms, for example `orders` and
`orders -> payments`.

For a resource query, only an exact, unambiguous node-name match is eligible.
Prefix and substring matching do not select a node. If no exact match exists,
or if the exact name identifies more than one node, the query is rejected with
an actionable message and the graph remains unchanged.

An itinerary may use any indexed node type as a stop or endpoint, including a
microservice, Kafka topic, MongoDB collection, or an external service when it
is present in the graph. For example:

```text
orders -> orders.created -> payments -> payment-events
```

When several routes satisfy the query, the UI displays the shortest directed
path that respects the specified stops. Listing alternative simple paths is not
part of this primary interaction.

The route computation uses the supplied stops as exact-name constraints. It
does not substitute approximate matches or silently remove a stop. If no
directed path passes through the supplied nodes in order, the UI reports that
no path exists.

### Acceptance criteria

- The entry point makes the two intents understandable: search a resource or
  search an itinerary.
- An itinerary can be entered from Explore with a visible example of its
  syntax.
- Invalid, non-exact, or ambiguous input and an impossible itinerary produce
  an actionable message and never change the graph silently.
- The UI reports actionable validation feedback for an empty stop, a trailing
  `->`, a repeated stop, an unknown node, and an ambiguous exact node name.
- Existing resource search remains fast and does not require route syntax.
- The interaction remains usable by keyboard and at constrained viewport
  heights.

## UX-002 — Explain the data flow of a displayed itinerary

**Status:** Implemented  
**Priority:** High

**Depends on:** UX-001 — Restore route discovery from Explore

### Problem

A highlighted path currently shows graph nodes, but it does not provide enough
context to understand the data flow. A user needs to see which services and
topics participate, and which DTOs are exchanged at each Kafka topic.

### Intended outcome

The path detail becomes an ordered, readable explanation of the selected data
flow rather than a bare sequence of graph nodes.

### Acceptance criteria

- Show every path stop in order, with its node type: microservice, Kafka topic,
  MongoDB collection, or external service.
- For each Kafka topic on the path, show the DTO types published to that topic
  and the types declared by the consumer services reached by the path.
- Show the services and topics involved in a compact ordered summary before
  lower-level technical evidence.
- Make unavailable or unresolved DTO information explicit; do not infer a
  message type.
- Keep the detail scoped to the selected path, not to every relation of every
  node in the whole graph.
- If a topic has producers or consumers outside the selected path, indicate
  that additional participants exist without expanding them into the primary
  flow explanation.
- Reveal producer/consumer source locations and file links only after the user
  selects the relevant path stop.
