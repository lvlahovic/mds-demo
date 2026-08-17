# Order API + Inventory Processing Service

A small event-driven system made of two independent Spring Boot services that
communicate through Redis Streams:

- **order-service** exposes `POST /orders` and publishes an order event for
  every accepted request.
- **inventory-service** consumes those events, simulates a stock reservation
  (enough quantity on hand → reserved, otherwise rejected) and keeps
  inventory in memory.

The brief for this exercise scoped evaluation to the **quality and
reliability of the integration** between the two services, not the business
logic itself - the design choices below follow from that.

## Running it

Requires Docker (Docker Desktop on Windows/Mac, or Docker Engine + the
`compose` plugin on Linux). No local JDK or Maven install is needed - the
build happens inside the Docker build stage.

```bash
docker compose up --build
```

This starts three containers:

| Service            | Port  | Role                                         |
|---------------------|-------|-----------------------------------------------|
| `redis`             | 6379  | Broker (Redis Streams), AOF persistence on   |
| `order-service`     | 8080  | `POST /orders`                                |
| `inventory-service` | -     | Background consumer, no HTTP surface          |

Send a request (also available as `order-service/src/main/java/com/lvl/mds/orderapi/request.http`
for IntelliJ's HTTP client):

```bash
curl -i -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"order-1","itemId":"item-1","quantity":2}'
```

`order-service` responds `202 Accepted` as soon as the event is durably
published - that only means the order was handed off, not that stock was
reserved. Watch `docker compose logs -f inventory-service` to see the actual
reservation outcome (`RESERVED` / rejected for insufficient stock / rejected
for an unknown item).

Seeded inventory (`inventory-service`, in-memory, resets on restart):

| itemId   | quantity |
|----------|----------|
| item-1   | 100      |
| item-2   | 50       |
| item-3   | 5        |

## Architecture decisions

### Redis Streams, not Redis Pub/Sub

Redis Pub/Sub has no persistence and no redelivery: if `inventory-service`
is down (or mid-restart) when an event is published, it's simply lost. That
fails the "reliable integration" requirement outright. Redis Streams keep
every entry (backed by AOF, see below), support consumer groups with
per-message acknowledgment, and let a message that was never acknowledged be
reclaimed and retried. This is also the pattern used for the equivalent
problem in my day job, so it's the one I can defend in the most depth.

### Reliability mechanism

- **At-least-once delivery, explicit ack.** `inventory-service` reads via
  `XREADGROUP` on the `inventory-service-group` consumer group and only
  `XACK`s a message after it has been fully handled. If processing throws,
  the message is deliberately left unacknowledged - it stays in the
  group's Pending Entries List (PEL) instead of being lost.
- **Idempotency.** Because consumer-group redelivery (or a producer retry)
  can hand the same `orderId` to `inventory-service` twice,
  `ProcessedOrdersStore` keeps an in-memory `Set<String>` of already-handled
  `orderId`s. A duplicate delivery is recognized, skipped, and still
  acknowledged.
- **Retry / dead-letter queue.** `PendingMessagesReclaimer` is a
  `@Scheduled` job that inspects `XPENDING` every `inventory.retry.scan-interval-ms`
  (default 10s) for entries idle longer than
  `inventory.retry.pending-threshold-ms` (default 30s) - e.g. because the
  consumer crashed mid-processing. It `XCLAIM`s those entries and retries
  them through the same processing path the live consumer uses. Once a
  message's delivery count exceeds `inventory.retry.max-attempts` (default
  3), it is written to `orders-stream-dlq` instead and acknowledged off the
  original stream, so a permanently-failing message can't spin forever.
- **AOF persistence.** `redis-server --appendonly yes` so `orders-stream`
  (and the consumer group's cursor/PEL) survive a Redis container restart.
- **`redis:8-alpine`.** Redis was dual-licensed under RSALv2/SSPLv1 (not an
  OSI-approved open-source license) from v7.4 through v7.x. Redis 8 (May
  2025) restored an AGPLv3 option, which is OSI-approved - so the compose
  file pins `redis:8-alpine` specifically to satisfy the task's "open-source
  broker" requirement, not just "any Redis image".

### Sibling folders, not a Maven multi-module build

`order-service` and `inventory-service` are two independent Maven projects
under one Git repository, each with its own `pom.xml`, Maven wrapper, and
`Dockerfile` - not a multi-module build with a shared parent `pom.xml`. The
brief calls these out as two independent services communicating over a
broker; a shared parent/reactor build would imply a build-time coupling
that doesn't reflect that (and that a real deployment wouldn't have either -
each service ships and versions on its own). Duplicating a few `pom.xml`
lines is a small price for keeping that boundary honest.

### Package layout

Each service follows the same horizontal/layered convention:
`model` (domain types), `dto` (request/response payloads), `repository`
(persistence - in-memory here), `services` (business logic), `web`
(controllers), `config` (wiring/beans), and a `messaging` package dedicated
to the Redis Streams integration - kept separate from `config` because it's
the part most specific to this task's broker choice.

## Manual test scenarios

These were exercised by hand against `docker compose up --build`:

1. **Happy path** - `POST /orders` with `quantity` within stock →
   `202 Accepted`, `inventory-service` logs `RESERVED`, available quantity
   for that item decreases.
2. **Insufficient stock** - request a quantity above what's seeded (e.g.
   `item-3`, seeded at 5, requested at 999) → event is still published and
   `202 Accepted` returned (publishing only means "accepted for
   processing"), `inventory-service` logs a rejection for insufficient
   stock; no quantity is deducted.
3. **Consumer crash mid-processing** - stop `inventory-service`
   (`docker compose stop inventory-service`) while a message is in-flight
   or unacknowledged, then start it back up
   (`docker compose start inventory-service`). The message is still in the
   PEL; `PendingMessagesReclaimer` claims and reprocesses it once it has
   been idle past the threshold, without needing anything republished by
   `order-service`.
4. **Duplicate delivery** - the same `orderId` arriving twice (simulated by
   re-publishing manually via `redis-cli XADD`) is reserved once; the
   second delivery is logged as a skipped duplicate and still acknowledged.

## Assumptions

- No database is required or used - both services are explicitly allowed
  to keep state in memory per the task description, so it resets on
  restart (inventory levels, processed-order idempotency set, and Redis
  Streams if the `redis-data` volume is removed).
- `order-service` validates `orderId`/`itemId` (non-blank) and `quantity`
  (positive integer) and returns `400 Bad Request` on invalid input,
  before anything is published.
- A single `inventory-service` consumer instance is assumed. The consumer
  group mechanics (and the `consumerName` property) support scaling to
  multiple instances for parallel processing, but that wasn't a stated
  requirement and hasn't been load-tested here.
- "Reservation" is simulated: it decrements an in-memory counter, it does
  not model holds, expiry, or compensating release-on-cancel flows.

## AI usage note

AI tooling (Claude) was used throughout development - as permitted by the
task description - for scaffolding, implementation, and drafting this
README. Architectural decisions (Redis Streams vs. Pub/Sub, the
sibling-folder repo layout, the retry/DLQ design) were directed and
reviewed, and I can walk through the reasoning behind each of them.
