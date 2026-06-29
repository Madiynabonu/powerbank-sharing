# DECISIONS.md — Powerbank Sharing MVP

## Architecture Decisions

### UUID vs BIGSERIAL
All primary keys are `UUID` (v4). Rationale:
- IDs are generated in application code before the DB insert, so Kafka events can carry the ID without a DB round-trip.
- Merging data across microservices is safe — no integer collision across schemas.
- Trade-off: UUID B-tree index performance degrades on very large tables (100M+ rows) because random v4 values cause page splits and poor cache locality. At that scale I would switch to UUID v7 (time-ordered) or ULID to restore sequential insertion order while keeping global uniqueness.

### NUMERIC vs Double for Money
All monetary columns use `NUMERIC(19,2)` in PostgreSQL and `BigDecimal` in Java. A `double` cannot represent 0.10 exactly in binary floating point, which causes rounding errors when summing many small values — unacceptable for financial ledgers. `NUMERIC` is exact decimal arithmetic.

### TIMESTAMPTZ vs TIMESTAMP
`TIMESTAMPTZ` stores the moment in UTC and adjusts display to the session's timezone. `TIMESTAMP` (without timezone) stores a local wall-clock value with no timezone context, so the meaning changes if the DB or application server moves to a different timezone. For an app that could run globally, `TIMESTAMPTZ` is the only safe choice.

### Index Strategy
| Table | Index | Reason |
|-------|-------|--------|
| `cards` | `idx_cards_user_id` | List all cards for a user |
| `payments` | `idx_payments_card_id` | Billing history per card |
| `payments` | `idx_payments_rental_id` | All payments for one rental |
| `payments` | `idx_payments_user_id` | User-facing payment history |
| `payments` | `uq_payments_idempotency_key` | Idempotency dedup |
| `stations` | `idx_stations_lat_lng` | Narrow candidate set for Haversine nearby query |
| `stations` | `idx_stations_status` | Filter ONLINE-only stations |
| `powerbanks` | `idx_powerbanks_station_id` | Find powerbanks in a station |
| `powerbanks` | `idx_powerbanks_status` | Find DOCKED powerbanks globally |
| `rentals` | `idx_rentals_user_id` | Rental history per user |
| `rentals` | `idx_rentals_status` | Recurrent charge: find IN_THE_LEASE |
| `rentals` | `idx_rentals_powerbank_id` | Look up which rental has a powerbank |
| `otp_codes` | `idx_otp_phone_expires` | Find latest valid OTP fast |

---

## Kafka Design

### What happens if Kafka is unavailable during publish?
The producer is configured with `acks=all` and `retries=10` with an idempotent producer (`enable.idempotence=true`). If Kafka is temporarily unavailable the send call blocks and retries. If all retries are exhausted, the exception propagates out of the Kafka listener (for results) or out of the service method (for commands), the Kafka offset is NOT committed, and the message is redelivered — giving us at-least-once processing. The idempotency key on payments ensures no double charge on redelivery.

Remaining gap: if the DB write commits but the Kafka publish fails completely, the rental or payment state advances but the downstream service never hears about it. The proper fix is the **Outbox pattern**: write the event to an `outbox` table in the same DB transaction, then have a separate relay process publish it to Kafka. For this MVP we accept the small window.

### Kafka Key Choice
| Topic | Key | Reason |
|-------|-----|--------|
| `acquire-cabinet-lock-event` | `rentalId` | All lock commands for one rental → same partition → ordered |
| `acquire-cabinet-lock-result` | `rentalId` | Result arrives after command on the same partition |
| `eject-powerbank-event` | `rentalId` | Ordered after the lock result |
| `eject-powerbank-result` | `rentalId` | Ordered with eject command |
| `payment-request` | `rentalId` | One payment request per rental, ordered |
| `payment-result` | `rentalId` | Arrives after request on same partition |
| `payment-events` | `cardId` | Audit stream: all events for one card in order |
| `card-command` | `cardId` | All commands for one card serialized |

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

1. **Outbox pattern** for payment and station events — eliminate the DB/Kafka inconsistency window.
2. **Real Telegram OTP** — implement the `/start` → `chat_id` registration flow and map phone numbers to Telegram chat IDs.
3. **Kong OIDC plugin** — replace the stub `oidc` config with a fully tested `lua-resty-openidc` setup and verify introspection end-to-end.
4. **gRPC transcoding** — test the Kong `grpc-gateway` plugin with the actual `.proto` files uploaded; verify field mapping for list queries.
5. **Integration tests** — use `@SpringBootTest` + Testcontainers for real Postgres/Kafka round-trip tests.
6. **Return flow with Station** — publish a return event to station-service so it can lock the returning slot and update the powerbank status.
7. **Retry / compensation** — if the lock succeeds but payment fails, release the lock (currently the powerbank stays in EJECTING state forever).
8. **Metrics** — expose Micrometer metrics (rental duration, payment failure rate) and wire to Prometheus/Grafana.
9. **Rate limiting** — add Kong rate-limit plugin to `/auth/phone` to prevent OTP spam.
10. **UUID v7** — replace random v4 with time-ordered v7 to improve B-tree index locality at scale.

---

## Questions

1. **Station return flow**: when a user returns a powerbank to *a different* station, should station-service validate that the slot is free before accepting? The FSM currently trusts the rental-service's `returnStationId`. Is there a hardware lock-and-confirm step on return too?
2. **Recurrent payment cadence**: the spec says "recurrent payment" without a specific interval. I chose 30 minutes as a reasonable MVP default. Is there a business rule (e.g., charge every N km, charge by battery level, charge at flat rate per day)?
3. **Kong OIDC plugin**: the open-source Kong image does not include `lua-resty-openidc` by default. Production would need either Kong EE or a custom image. Should I add a Dockerfile that installs the plugin, or is Kong EE assumed?
4. **Telegram OTP**: OTP delivery requires knowing the user's Telegram `chat_id`. This means the user must first message the bot to register. Is there a separate registration endpoint, or should the `/auth/phone` flow deep-link to the bot with a magic token?
5. **Geo-index**: I used a Haversine formula in JPQL. For a production system with thousands of stations, PostGIS with a spatial index would be far more efficient. Should I add PostGIS to the station-service DB?
