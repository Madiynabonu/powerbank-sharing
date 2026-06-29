# DECISIONS.md — Powerbank Sharing MVP

## Architecture Decisions

### Why Microservices and How Services Are Split
Four services map cleanly to four bounded contexts with different scaling, persistence, and communication needs:

| Service | Reason for separation |
|---|---|
| `user-service` | Auth boundary — owns identity, OTP, Keycloak integration. Must stay isolated from business logic. |
| `payment-service` | Financial boundary — atomicity and auditability requirements differ from other domains. Kafka-only keeps it decoupled from callers. |
| `station-service` | IoT boundary — async hardware simulation, independent deployability. |
| `rental-service` | Orchestration boundary — owns the FSM that coordinates all other services. |

### Database per Service
Each service has its own PostgreSQL instance. Shared databases create invisible coupling — a schema change in one service can break another at runtime with no compile-time signal. Separate DBs enforce the contract boundary: services can only communicate via API (gRPC or Kafka), never via JOIN.

### Why Kafka for Some, gRPC for Others
- **Kafka** (rental↔station, rental↔payment): station and payment operations are **asynchronous** by nature (hardware latency, card processing). Kafka decouples producer and consumer, allows retry, and provides a durable audit log.
- **gRPC** (Kong→rental, rental→station): synchronous request-response for client-facing reads (station list, rental status). gRPC gives strong typing via proto contracts and Kong can transcode REST→gRPC at the gateway level without writing REST controllers in each service.

### UUID vs BIGSERIAL
All primary keys are `UUID` (v4). Rationale:
- IDs are generated in application code before the DB insert, so Kafka events can carry the ID without a DB round-trip.
- Merging data across microservices is safe — no integer collision across schemas.
- Trade-off: UUID B-tree index performance degrades on very large tables (100M+ rows) because random v4 values cause page splits and poor cache locality. At that scale I would switch to UUID v7 (time-ordered) or ULID to restore sequential insertion order while keeping global uniqueness.

### NUMERIC vs Double for Money
All monetary columns use `NUMERIC(19,2)` in PostgreSQL and `BigDecimal` in Java. A `double` cannot represent 0.10 exactly in binary floating point, which causes rounding errors when summing many small values — unacceptable for financial ledgers. `NUMERIC` is exact decimal arithmetic.

### TIMESTAMPTZ vs TIMESTAMP
`TIMESTAMPTZ` stores the moment in UTC and adjusts display to the session's timezone. `TIMESTAMP` (without timezone) stores a local wall-clock value with no timezone context, so the meaning changes if the DB or application server moves to a different timezone. For an app that could run globally, `TIMESTAMPTZ` is the only safe choice.

### Pessimistic Locking in PaymentService
`PaymentService` uses `SELECT FOR UPDATE` (pessimistic row lock) when charging a card. The alternative — optimistic locking with `@Version` — would throw `OptimisticLockException` on concurrent charges for the same card and require the caller to retry. For a payment operation, silently retrying with a stale balance is dangerous. Pessimistic locking serializes concurrent charges at the DB level and guarantees the balance check and debit happen atomically without retry logic in the application layer. The `@Version` field is kept as a safety net for code paths that do not take the explicit lock.

### Why Quartz Instead of @Scheduled
Recurrent payments use Quartz with a PostgreSQL JDBC job store instead of Spring's `@Scheduled`. Reasons:
- `@Scheduled` is in-memory — a service restart resets the timer, potentially skipping a charge cycle.
- Quartz persists triggers to the DB — a restart picks up from where it left off.
- Quartz is cluster-safe — if multiple instances run, only one fires the job per interval (via DB-level lock on the trigger row).

### Why Keycloak
Keycloak is the most complete open-source OAuth2/OIDC server available as a Docker image. It provides: user management, ROPC grant for phone+password login, token introspection endpoint (used by Kong), and realm import for reproducible environments. The alternative (writing a custom JWT issuer) would duplicate security-sensitive code that Keycloak handles correctly out of the box.

### Station Endpoints via Rental Service
`GET /v1/stations` and `GET /v1/stations/{id}` are exposed through the **rental-service** gRPC boundary, which internally calls station-service via a gRPC client stub. Kong routes all `/v1/stations` traffic to rental-service (grpc-gateway transcodes REST→gRPC), and rental-service acts as the single client-facing gRPC gateway for both rental and station operations. Station-service has no public HTTP routes — it is an internal service called only by rental-service.

### Kong OIDC Plugin
A custom Kong Dockerfile (`infra/kong/Dockerfile`) installs `lua-resty-openidc` and its dependencies (`lua-resty-http`, `lua-resty-session 3.x`) via luarocks on top of `kong:3.7-ubuntu`. This avoids the need for Kong Enterprise and makes the OAuth2 Token Introspection plugin available in the open-source image. The `oidc` plugin in `kong.yaml` calls Keycloak's introspection endpoint with `bearer_only: yes`, meaning Kong validates the JWT on every `/v1/*` request before forwarding to backend services.

### Kafka KRaft Mode (No ZooKeeper)
Kafka runs in KRaft mode (`KAFKA_PROCESS_ROLES: broker,controller`) — the built-in Raft consensus introduced in Kafka 3.x that replaces ZooKeeper. This removes one entire process from docker-compose and is the direction Kafka is heading for production clusters. For a single-node MVP setup the simplification is significant; KRaft has been production-ready since Kafka 3.3 so this is low-risk.

### Haversine in JPQL Instead of PostGIS
Nearby-station queries use an `acos(cos(...)...)` Haversine formula in a JPQL `@Query`. PostGIS would give a proper spatial index but requires adding the extension to the PostgreSQL container and increases operational complexity. For city-scale distances (<100 km) Haversine is accurate to <0.1%. The composite `(lat, lng)` index pre-narrows the candidate set. The trade-off (no spatial index on large datasets) is acknowledged in the Questions section.

### Liquibase Over Flyway
All four services use `org.liquibase:liquibase-core` with `ddl-auto: none` — Hibernate never touches DDL. Liquibase YAML changelogs are more readable than SQL when mixing DDL + DML seed data, and the checksum mechanism prevents changeset re-execution without extra `ON CONFLICT DO NOTHING` guards. Quartz schema initialization is also delegated to Liquibase (`initialize-schema: never`) so the entire schema history — including scheduler tables — lives in one migration log.

### Seed Data via Liquibase Changeset
Demo stations, powerbanks, and payment cards are inserted in versioned Liquibase changesets (e.g. `002-seed-demo-data.yaml`), not in a `data.sql` or `@PostConstruct` bean. A changeset runs exactly once (checksum-gated) and appears in the migration history. A `data.sql` would re-run on every restart unless manually guarded.

### No Kafka `__TypeId__` Headers — Plain JSON
All producer/consumer configs set `ADD_TYPE_INFO_HEADERS: false` and `USE_TYPE_INFO_HEADERS: false`. Spring Kafka normally embeds a `__TypeId__` header with the producer's fully-qualified class name, which couples consumers to the producer's package structure — a class rename on the producer side silently breaks deserialization on the consumer. With headers disabled, the payload is plain JSON and each listener factory pins the target type explicitly (`VALUE_DEFAULT_TYPE`). Producers and consumers can evolve independently.

### AckMode.RECORD + Dead-Letter Topic After 3 Retries
All Kafka listeners use `AckMode.RECORD` (offset committed after each record, not after the batch) and `ENABLE_AUTO_COMMIT_CONFIG: false`. On failure, `FixedBackOff(1000 ms, 3 attempts)` retries three times, then `DeadLetterPublishingRecoverer` forwards the message to `<topic>.DLT`. This ensures: (1) a failure on record N does not cause records 0..N-1 to be reprocessed; (2) a poison-pill message does not block the partition forever; (3) failed messages are preserved in the DLT for inspection and replay.

### `saveAndFlush` for Idempotency Race Condition
`PaymentService` uses `saveAndFlush()` instead of `save()` when inserting a payment. `save()` defers the SQL INSERT until the transaction commits; if a unique-constraint violation on `idempotency_key` surfaces at that point it is uncatchable in the method body. `saveAndFlush()` forces an immediate flush so the `DataIntegrityViolationException` is caught inside the try/catch and re-queried, making the concurrent-duplicate path work correctly.

### `open-in-view: false`
All services set `jpa.open-in-view: false`. Spring Boot defaults this to `true`, which keeps the Hibernate session open for the full HTTP request lifetime, enabling lazy-loading outside `@Transactional` boundaries and holding a DB connection during view rendering. With microservices there is no view layer, so the open session wastes connections and masks N+1 bugs. Disabling it forces all DB access to stay within explicit transaction boundaries.

### IoT Hardware Simulation with Configurable Failure Rate
`station-service` has no real hardware. `simulateLock()` and `simulateEject()` call `Thread.sleep()` (500 ms / 800 ms) to model async latency and inject a 5% random failure via `ThreadLocalRandom`. All three values (`lock-sim-delay-ms`, `eject-sim-delay-ms`, `failure-rate`) are externalised to `application.yaml`. This makes the compensating code paths (cancel-payment-command, rollback to DOCKED) exercisable in the running system without any hardware.

### Recurrent Payment Interval: 30 Minutes at 1 000 UZS
The spec mandates recurrent payment but does not specify an interval or amount. 30 minutes and 1 000 UZS were chosen as a sensible MVP default — frequent enough to test the scheduler, low enough not to drain the demo card balance immediately. Both values are externalised via `@Value` (`recurrent-interval-minutes`, `recurrent-amount`) so they can be overridden without recompile.

### Index Strategy
| Table | Index | Reason |
|---|---|---|
| `cards` | `idx_cards_user_id` | List all cards for a user |
| `payments` | `idx_payments_card_id` | Billing history per card |
| `payments` | `idx_payments_rental_id` | All payments for one rental |
| `payments` | `idx_payments_user_id` | User-facing payment history |
| `payments` | `uq_payments_idempotency_key` | Idempotency dedup — unique constraint is the ground truth |
| `stations` | `idx_stations_lat_lng` | Narrow candidate set for Haversine nearby query |
| `stations` | `idx_stations_status` | Filter ONLINE-only stations |
| `powerbanks` | `idx_powerbanks_station_id` | Find powerbanks in a station |
| `powerbanks` | `idx_powerbanks_status` | Find DOCKED powerbanks globally |
| `rentals` | `idx_rentals_user_id` | Rental history per user |
| `rentals` | `idx_rentals_status` | Recurrent charge: find IN_THE_LEASE rentals |
| `rentals` | `idx_rentals_powerbank_id` | Look up which rental currently holds a powerbank |
| `otp_codes` | `idx_otp_phone_expires` | Find latest valid OTP fast |

---

## Kafka Design

### What happens if Kafka is unavailable during publish?
The producer is configured with `acks=all` and `retries=10` with an idempotent producer (`enable.idempotence=true`). If Kafka is temporarily unavailable the send call blocks and retries. If all retries are exhausted, the exception propagates — the Kafka consumer offset is NOT committed, so the message is redelivered giving us at-least-once processing. The idempotency key on payments ensures no double charge on redelivery.

Remaining gap: if the DB write commits but the Kafka publish fails completely, the rental or payment state advances but the downstream service never hears about it. The proper fix is the **Outbox pattern**: write the event to an `outbox` table in the same DB transaction, then have a separate relay process (e.g. Debezium) publish it to Kafka. For this MVP we accept the small window.

### Kafka Key Choice
| Topic | Key | Reason |
|---|---|---|
| `acquire-cabinet-lock-event` | `rentalId` | All lock commands for one rental → same partition → ordered |
| `acquire-cabinet-lock-result` | `rentalId` | Result arrives after command on the same partition |
| `eject-powerbank-event` | `rentalId` | Ordered after the lock result |
| `eject-powerbank-result` | `rentalId` | Ordered with eject command |
| `payment-request` | `rentalId` | One payment request per rental, ordered |
| `payment-result` | `rentalId` | Arrives after request on same partition |
| `payment-events` | `cardId` | Audit stream: all events for one card in order |
| `card-command` | `cardId` | All commands for one card serialized |
| `cancel-payment-command` | `rentalId` | Cancel after eject failure; ordered with payment-request |
| `return-powerbank-command` | `rentalId` | Return to station after finish; ordered per rental |

Ordering matters because rental-service processes FSM transitions from multiple topics; if the lock result arrived after the eject command we would try to eject before locking.

### DB + Kafka Transactionality
The ideal solution is the **Outbox pattern**: write the domain change and the Kafka event in one DB transaction; a separate relay (e.g., Debezium CDC) publishes the event. Without Outbox there is a window between the DB commit and the Kafka send where a crash loses the event.

For this MVP:
- We use `acks=all` + idempotent producer to avoid duplicates on retry.
- We do NOT commit the Kafka consumer offset until after the DB write succeeds (`AckMode.RECORD`), so a DB failure causes message redelivery.
- The remaining gap (DB commits, Kafka send fails) is acknowledged — see "What I Would Do With More Time".

---

## Idempotency

### idempotency_key is UNIQUE — how is the conflict handled?
The unique constraint is the ground truth. The application first calls `findByIdempotencyKey(key)`, and if found returns the existing record. The race condition (two concurrent identical requests both see "not found") is closed by catching `DataIntegrityViolationException` on the failed insert and re-querying for the winner.

### Status code: 200 or 201 on duplicate?
**200** is returned for a duplicate idempotency_key. 201 means "a new resource was created". On a duplicate request, nothing new was created — returning 201 would be misleading. RFC-compliant clients that distinguish 200 vs 201 would get the correct signal.

### Same key, different amount?
We return the **original** payment unchanged and log a warning. The idempotency contract is: "same key = same operation". Changing the amount invalidates the semantic of the key. We do NOT apply the new amount, do NOT fail the request — we return the original, which is the behaviour Stripe and other payment APIs use.

---

## What I Would Do With More Time

1. **Outbox pattern** — eliminate the DB/Kafka inconsistency window for payment and station events.
2. **Real Telegram OTP** — implement the `/start` → `chat_id` registration flow and map phone numbers to Telegram chat IDs.
3. **Lock release on payment failure** — when payment fails after a successful lock, the powerbank stays in EJECTING state forever. A compensating command back to station-service is needed to reset it to DOCKED.
4. **Integration tests** — `@SpringBootTest` + Testcontainers for real Postgres/Kafka round-trip tests covering the full FSM.
5. **gRPC transcoding e2e test** — verify Kong `grpc-gateway` field mapping for list queries and pagination parameters.
6. **Metrics** — expose Micrometer metrics (rental duration, payment failure rate, charge success rate) and wire to Prometheus/Grafana.
7. **Rate limiting** — add Kong rate-limit plugin to `/auth/phone` to prevent OTP spam.
8. **UUID v7** — replace random v4 with time-ordered v7 to improve B-tree index locality at scale.

---

## Questions

1. **Station return flow**: when a user returns a powerbank to *a different* station, should station-service validate that the slot is free before accepting? The FSM currently trusts the rental-service's `returnStationId`. Is there a hardware lock-and-confirm step on return too?
2. **Recurrent payment cadence**: the spec says "recurrent payment" without a specific interval. I chose 30 minutes as a reasonable MVP default. Is there a business rule (e.g., charge every N km, charge by battery level, charge at flat rate per day)?
3. **Telegram OTP**: OTP delivery requires knowing the user's Telegram `chat_id`. This means the user must first message the bot to register. Is there a separate registration endpoint, or should the `/auth/phone` flow deep-link to the bot with a magic token?
4. **Geo-index**: I used a Haversine formula in JPQL. For a production system with thousands of stations, PostGIS with a spatial index would be far more efficient. Should I add PostGIS to the station-service DB?
