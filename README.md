# Powerbank Sharing — MVP

Powerbank ijarasi uchun mikroservis arxitekturasi.

## Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 |
| Framework | Spring Boot 3.4.2 |
| Database | PostgreSQL 16 |
| Messaging | Apache Kafka 7.6 (KRaft) |
| Auth | Keycloak 24 |
| API Gateway | Kong 3.7 (DB-less) |
| IPC | gRPC 1.75 (station-service, rental-service) |
| Migration | Liquibase |
| Scheduler | Quartz JDBC |
| Container | Docker Compose |

## Architecture

```
Client
  └── Kong Gateway (port 8000)
        ├── /auth/**          → user-service:8080   (HTTP REST)
        ├── /v1/me            → user-service:8080   (JWT introspection)
        ├── /v1/stations/**   → station-service:9092 (gRPC transcoding)
        └── /v1/rental/**     → rental-service:9091  (gRPC transcoding)

Kafka topics:
  acquire-cabinet-lock-event   rental→station
  acquire-cabinet-lock-result  station→rental
  eject-powerbank-event        rental→station
  eject-powerbank-result       station→rental
  payment-request              rental→payment
  payment-result               payment→rental
  payment-events               payment→(audit)
  card-command                 *→payment (bind card)
```

## Quickstart

```bash
# 1. Build and start everything
docker compose up --build

# 2. Wait for all services to be healthy (~2-3 minutes first run)
docker compose ps

# 3. Test: request OTP
curl -X POST http://localhost:8000/auth/phone \
  -H 'Content-Type: application/json' \
  -d '{"phone": "+998901234567"}'

# OTP appears in user-service logs:
docker compose logs user-service | grep "OTP"

# 4. Verify OTP, get JWT
curl -X POST http://localhost:8000/auth/verify \
  -H 'Content-Type: application/json' \
  -d '{"phone": "+998901234567", "code": "<OTP_FROM_LOGS>"}'

# 5. Use the access_token from step 4
export TOKEN=<access_token>

# 6. Get nearby stations
curl "http://localhost:8000/v1/stations?lat=41.299&lng=69.240&radius_m=5000" \
  -H "Authorization: Bearer $TOKEN"

# 7. Create a rental (use stationId from step 6, cardId from demo seed)
curl -X POST http://localhost:8000/v1/rental \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "station_id": "aaaaaaaa-0001-0001-0001-aaaaaaaaaaaa",
    "card_id":    "11111111-1111-1111-1111-111111111111",
    "user_id":    "00000000-0000-0000-0000-000000000001"
  }'

# 8. Poll rental status (replace <rental_id> with value from step 7)
curl "http://localhost:8000/v1/rental/<rental_id>/status" \
  -H "Authorization: Bearer $TOKEN"
# Wait for status to reach IN_THE_LEASE

# 9. Finish rental
curl -X POST http://localhost:8000/v1/rental/finish \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "rental_id":  "<rental_id>",
    "station_id": "aaaaaaaa-0002-0002-0002-aaaaaaaaaaaa",
    "user_id":    "00000000-0000-0000-0000-000000000001"
  }'
```

## Local Maven build (without Docker)

> Requires PostgreSQL + Kafka running locally.

```bash
# use project settings.xml to bypass corporate Nexus
cd payment-service && ./mvnw -s ../.mvn/settings.xml clean test
cd ../station-service && ./mvnw -s ../.mvn/settings.xml clean test
cd ../rental-service  && ./mvnw -s ../.mvn/settings.xml clean test
cd ../user-service    && ./mvnw -s ../.mvn/settings.xml clean test
```

## Demo seed data

| Service | Data |
|---------|------|
| station-service | 2 stations (Tashkent Mall, Chorsu Metro), 6 powerbanks |
| payment-service | 2 demo cards (balance 500 000 UZS and 1 000 UZS) |

## Ports

| Service | HTTP | gRPC |
|---------|------|------|
| Kong proxy | 8000 | — |
| Kong admin | 8001 | — |
| user-service | 8080 | — |
| rental-service | 8081 | 9091 |
| station-service | 8082 | 9092 |
| payment-service | 8083 | — |
| Keycloak | 8180 | — |
| Kafka (external) | 29092 | — |

## Architecture decisions

See [DECISIONS.md](DECISIONS.md).
