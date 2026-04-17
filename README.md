# OmniNOC Fault Management Platform

[Türkçe Versiyonu (README.tr.md)](README.tr.md)

This is an event-driven microservices backend I built for alarm ingestion and correlation. It basically simulates how a real Network Operations Center (NOC) handles massive alarm storms and boils them down into actionable incidents.

## The Problem
Imagine a single fiber cut happens. Suddenly, hundreds of alarms trigger across transport, access, and service layers. If you don't correlate them, NOC operators just drown in noise.

**The Goal:** Group all these related alarms into a single incident based on root cause, and assign it to the right team automatically.

## How It Works
```
OSS/EMS/NMS  --POST-->  alarm-ingestion-service  --Kafka-->  correlation-engine
                             (port 8081)           alarm.normalized   (port 8082)
                                                                          |
                                                               Redis (sliding window)
                                                               PostgreSQL (incidents)
                                                               Kafka (incident.updated)
```

- **alarm-ingestion-service**: Takes raw alarms via REST, normalizes them, and throws them onto a Kafka topic.
- **correlation-engine**: Consumes those alarms, applies correlation rules, and creates/updates incidents in Postgres. 

## Tech Stack
- **Java 21 & Spring Boot 3.3**
- **PostgreSQL 16 & Flyway** for persistence and migrations.
- **Apache Kafka 3.7** for async messaging.
- **Redis 7** for handling 10-minute sliding windows per site.
- **Docker Compose** to spin up the whole infrastructure locally.

## The Correlation Rules
I implemented four main rules in the engine:
1. **Time + Site Window:** If alarms come from the same site within 10 minutes, they get grouped into the same incident. I used Redis TTL for this, it's super clean.
2. **Severity Boost:** If the alarm is from a `CORE_ROUTER`, its severity gets bumped up automatically.
3. **Parent-Child Detection:** A `FIBER_CUT` opens an incident. If a `SERVICE_DEGRADED` comes right after from the same site, it's marked as a `CHILD` alarm of the fiber cut.
4. **Root Cause Decision:** The engine decides the root cause and the assigned team based on the first critical alarm type (e.g., Power Outage goes to Field Ops, Fiber Cut goes to Transmission).

## Quick Start

Just use docker-compose to bring up Kafka, Redis, and Postgres:
```bash
cd infra/docker-compose
docker-compose up -d
```

APIs will be available at:
- Ingestion: `http://localhost:8081/swagger-ui.html`
- Correlation: `http://localhost:8082/swagger-ui.html`

## Testing it out (Demo)

You can trigger a simulated fiber cut scenario using curl. Send the root cause first, then child alarms:

```bash
# 1. The Fiber Cut (Root Cause)
curl -X POST http://localhost:8081/api/v1/alarms/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sourceSystem": "OSS",
    "alarmType": "FIBER_CUT",
    "severity": "CRITICAL",
    "siteId": "IST-TRX-001",
    "region": "Istanbul",
    "deviceType": "TRANSMISSION_NODE",
    "deviceId": "IST-TRX-001",
    "description": "Fiber cut on main trunk"
  }'

# 2. The downstream effect (Child)
curl -X POST http://localhost:8081/api/v1/alarms/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sourceSystem": "NMS",
    "alarmType": "LINK_DOWN",
    "severity": "MAJOR",
    "siteId": "IST-TRX-001",
    "region": "Istanbul",
    "deviceType": "TRANSMISSION_NODE",
    "deviceId": "IST-TRX-001",
    "description": "Link down after fiber cut"
  }'
```
Then check the correlation stats at `http://localhost:8082/api/v1/correlation/stats`. You'll see it processed 2 alarms but only created 1 incident.

## Some Design Decisions
- **Kafka for everything:** Decouples the ingestion from processing. If there's a massive alarm storm, Kafka buffers it and the correlation engine works at its own pace.
- **Partitioning by siteId:** I made sure messages are keyed by `siteId`. This guarantees that alarms for the same site are processed in order by the same consumer thread.
- **Redis TTLs:** Instead of writing a cron job to clean up old alarm windows, I just use Redis keys with a 10-minute TTL. The database naturally expires the window.
