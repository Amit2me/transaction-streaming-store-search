# 💼 Transaction Streaming Demo
**Java 21 • Spring Boot 3.4.x • Kafka • Cassandra • ELK (Elasticsearch + Logstash + Kibana)**

Two microservices that simulate a payments pipeline:

- **tx-producer** → generates dummy transactions and **publishes to Kafka**
- **tx-consumer** → **consumes from Kafka** and writes to **Cassandra** (idempotent)
- Optional **ELK**: centralized JSON logs (Logstash → Elasticsearch → Kibana)
- Provided **Docker Compose** for Kafka (KRaft) + Kafka UI + Cassandra (+ keyspace init) and for ELK

> This README is **self-contained** and **step-by-step**. Follow it top-to-bottom to run on any machine with Docker, Java 21, and Maven.

---

## 0) Prerequisites

- Docker Desktop (Compose v2 enabled)
- JDK **21**
- Maven **3.9+**
- `curl` (or Postman)
- (Optional) `nc`/`netcat` for quick TCP tests

> Windows: WSL2 recommended in Docker Desktop.  
> Apple Silicon: OK (images used support arm64). If RAM is tight, use the **Resource Tuning** section.

---

## 1) Repository Layout (what you should have)

├─ docker-compose.infra.yml # Kafka + Kafka UI + Cassandra + keyspace init job
├─ docker-compose.elk.yml # Elasticsearch + Logstash (5000 in container; 5500 on host) + Kibana
├─ logstash/
│ └─ pipeline/
│ └─ logstash.conf # TCP JSON input → enrich → Elasticsearch
├─ tx-producer/ # Spring Boot 3.4.x (WebFlux + Spring Kafka)
│ ├─ pom.xml
│ ├─ src/main/java/...
│ └─ src/main/resources/logback-spring.xml # JSON logs + Logstash TCP appender
└─ tx-consumer/ # Spring Boot 3.4.x (WebFlux + Spring Kafka + Reactive Cassandra)
├─ pom.xml
├─ src/main/java/...
└─ src/main/resources/logback-spring.xml


> If any of these files are missing, copy them from your project’s templates. This README assumes they exist as above.

---

## 2) TL;DR (Quick Start)

```bash
# From the repo root

# 1) Infra: Kafka + Kafka UI + Cassandra + keyspace init
docker compose -f docker-compose.infra.yml up -d
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE txks"   # should list the keyspace

# 2) (Optional) ELK logs stack (host port 5500 → container 5000)
docker compose -f docker-compose.elk.yml up -d

# 3) Run producer (port 8086)
export LOGSTASH_HOST=127.0.0.1 LOGSTASH_PORT=5500   # only if ELK running
cd tx-producer && mvn -q spring-boot:run

# 4) Run consumer (port 8087)
export LOGSTASH_HOST=127.0.0.1 LOGSTASH_PORT=5500   # only if ELK running
cd ../tx-consumer && mvn -q spring-boot:run

# 5) Drive traffic
curl -X POST "http://localhost:8086/api/tx/burst?count=50"

# 6) Verify
curl "http://localhost:8087/api/tx/all"                         # up to 50 rows
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM txks.transactions;"
# Kafka UI: http://localhost:8085
# Kibana:   http://localhost:5601  (create data view: tx-logs-*)
