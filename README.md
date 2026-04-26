# Kafka Transactions

Event-driven payment transaction system using Kafka for microservice communication. Demonstrates transactional Kafka semantics, event-driven architecture, and microservice orchestration with Spring Boot.

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://www.oracle.com/java/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.1.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Spring Kafka](https://img.shields.io/badge/Spring%20Kafka-Enabled-brightgreen.svg)](https://spring.io/projects/spring-kafka)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Enabled-231F20.svg)](https://kafka.apache.org/)
[![Maven](https://img.shields.io/badge/Maven-3.8+-blue.svg)](https://maven.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)](https://www.docker.com/)

---

## Features

### Transfer Service

- **REST API** — POST `/transfers` initiates money transfers
- **Event Orchestration** — Publishes withdrawal then deposit events to Kafka
- **Remote Verification** — Calls external validation service before deposit
- **Transactional** — @Transactional ensures all-or-nothing semantics
- **Error Handling** — Automatic retry with exponential backoff via Spring Kafka

### Withdrawal Service

- **Kafka Consumer** — Listens on `withdraw-money-topic`
- **Account Debit** — Processes withdrawal events and debits sender account
- **Transactional Semantics** — READ_COMMITTED isolation level for consistency
- **Error Recovery** — Failed messages sent to dead-letter topic for replay

### Deposit Service

- **Kafka Consumer** — Listens on `deposit-money-topic`
- **Account Credit** — Processes deposit events and credits recipient account
- **Parallel Processing** — Runs independently from withdrawal service
- **Isolation Level** — READ_COMMITTED ensures only committed events consumed

### Core Module

- **Shared Events** — `DepositRequestedEvent`, `WithdrawalRequestedEvent`
- **Exception Classes** — `RetryableException`, `NotRetryableException` for error handling
- **Type Safety** — Centralized event contracts for all services

### Mock Service

- **Verification Stub** — HTTP endpoint for transfer validation
- **Testing Support** — Simulates external service responses
- **Port 8083** — Dedicated service port for integration testing

---

## Technology Stack

| Category | Technology |
|----------|-----------|
| **Language** | Java 17 |
| **Framework** | Spring Boot 3.3.0 |
| **Message Broker** | Apache Kafka |
| **Kafka Integration** | Spring Kafka + Spring Boot Starter |
| **Build Tool** | Maven 3.8+ |
| **Containerization** | Docker |
| **Database** | H2 (in-memory for dev/test) |
| **ORM** | Hibernate + Spring Data JPA |

---

## Quick Start

### Prerequisites

- **Java 17** or higher ([Download](https://www.oracle.com/java/technologies/downloads/#java17))
- **Maven 3.8+** (or use included wrapper)
- **Docker** (for Kafka, optional for local broker)
- **Apache Kafka** running on `localhost:9092`

### Option 1: Local Development (Maven)

#### 1. Clone the repository

```bash
git clone https://github.com/toganbayev/kafka-transactions.git
cd kafka-transactions
```

#### 2. Start Kafka (Port 9092)

Using Docker (recommended):

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  confluentinc/cp-kafka:latest
```

Or use local Kafka installation:

```bash
kafka-server-start.sh config/server.properties
```

#### 3. Build all modules

```bash
mvn clean install
```

#### 4. Start Transfer Service (Port 8080)

```bash
cd transfer-service
mvn spring-boot:run
```

#### 5. Start Deposit Service (Port 8081)

Open a new terminal:

```bash
cd deposit-service
mvn spring-boot:run
```

#### 6. Start Withdrawal Service (Port 8082)

Open a new terminal:

```bash
cd withdrawal-service
mvn spring-boot:run
```

#### 7. Start Mock Service (Port 8083)

Open a new terminal:

```bash
cd mock-service
mvn spring-boot:run
```

**Service Endpoints:**

- **Transfer API:** `http://localhost:8080/transfers`
- **Kafka Broker:** `localhost:9092`
- **Mock Service:** `http://localhost:8083/response/200`

### Option 2: Docker Deployment

#### Build and run all services

```bash
# Build images for all services
docker build -t kafka-transfer:latest ./transfer-service
docker build -t kafka-deposit:latest ./deposit-service
docker build -t kafka-withdrawal:latest ./withdrawal-service
docker build -t kafka-mock:latest ./mock-service

# Create Docker network
docker network create kafka-net

# Run Kafka
docker run -d --name kafka --network kafka-net \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  confluentinc/cp-kafka:latest

# Run services
docker run -d --name transfer --network kafka-net -p 8080:8080 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  kafka-transfer:latest

docker run -d --name deposit --network kafka-net -p 8081:8081 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  kafka-deposit:latest

docker run -d --name withdrawal --network kafka-net -p 8082:8082 \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  kafka-withdrawal:latest

docker run -d --name mock --network kafka-net -p 8083:8083 \
  kafka-mock:latest
```

### Accessing Services

**Transfer API:**

```bash
curl -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{"senderId":"user1","recepientId":"user2","amount":100.50}'
```

**Kafka Topics:**

```bash
# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume from deposit topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic deposit-money-topic --from-beginning
```

---

## API Documentation

### Transfer Service REST API

#### Initiate Transfer

**Endpoint:** `POST /transfers`

**Request Body:**

```json
{
  "senderId": "user123",
  "recepientId": "user456",
  "amount": 250.75
}
```

**Response (200 OK):**

```json
true
```

**Example:**

```bash
curl -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{
    "senderId": "user123",
    "recepientId": "user456",
    "amount": 250.75
  }'
```

**Error Handling (500 Internal Server Error):**

```json
{
  "timestamp": "2026-04-26T10:30:00Z",
  "status": 500,
  "error": "TransferServiceException",
  "message": "Failed to process transfer"
}
```

---

## Architecture

### Microservices Overview

```
┌─────────────────────────────────────────────────────────┐
│                    REST Client                          │
└──────────────────────┬──────────────────────────────────┘
                       │ POST /transfers
                       ▼
       ┌───────────────────────────┐
       │   Transfer Service        │ Port 8080
       │  - REST API               │
       │  - Event Orchestration    │
       │  - Dual Transaction Mgmt  │
       │  - Validation             │
       └────────┬─────────┬────────┘
                │         │
        Publish │         │ HTTP Call
      Withdrawal│         │
                │         ▼
                │  ┌──────────────────┐
                │  │   Mock Service   │ Port 8083
                │  │  (Validation)    │
                │  └──────────────────┘
                │
      ┌─────────▼──────────┐
      │   Kafka Broker     │ Port 9092
      │ withdraw-topic     │
      │ deposit-topic      │
      │ *.DLT (dead-letter)│
      └────┬────────────┬──┘
           │            │
    Consume│            │Consume
           │            │
    ┌──────▼───┐   ┌────▼──────┐
    │Withdrawal│   │ Deposit   │ Port 8081
    │Service   │   │ Service   │
    │Port 8082 │   │           │
    │- Debits  │   │- Credits  │
    └──────────┘   └───────────┘
           │            │
           └──────┬─────┘
                  │ (Account updates)
                  ▼
           [H2 Database]
           (transfers table,
            JPA persistence)
```

### Data Flow

1. **Initiate:** Client POSTs transfer request to Transfer Service
2. **Debit Step:** Transfer Service publishes `WithdrawalRequestedEvent`
3. **Verification:** Transfer Service calls MockService for validation
4. **Credit Step:** Transfer Service publishes `DepositRequestedEvent`
5. **Processing:** Both services consume events in parallel
   - Withdrawal Service debits sender's account
   - Deposit Service credits recipient's account
6. **Consistency:** READ_COMMITTED isolation ensures no dirty reads

### Event Model

**WithdrawalRequestedEvent:**

```java
class WithdrawalRequestedEvent {
    String senderId;           // Who money leaves from
    String recepientId;        // Where money goes to
    BigDecimal amount;         // Transaction amount
}
```

**DepositRequestedEvent:**

```java
class DepositRequestedEvent {
    String senderId;           // Source account holder
    String recepientId;        // Target account holder
    BigDecimal amount;         // Transaction amount
}
```

### Error Handling

**Retry Strategy:**

- **Retryable Exceptions** (`RetryableException`): Retry 3 times with 5-second backoff
- **Non-Retryable** (`NotRetryableException`): Skip to dead-letter topic immediately
- **Dead Letter Topic:** Messages failing all retries sent to `{topic-name}.DLT`

**Example Flow:**

```
WithdrawalRequestedEvent
    ↓ (Consumer processes)
    ├─ Success? → Account debited
    └─ Failure?
        ├─ RetryableException? → Retry (max 3x, 5s backoff)
        └─ NotRetryableException? → Send to withdraw-money-topic.DLT
```

---

## Configuration

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SPRING_KAFKA_CONSUMER_GROUP_ID` | `deposit-group` | Consumer group for coordination |
| `SPRING_KAFKA_CONSUMER_ISOLATION_LEVEL` | `READ_COMMITTED` | Transaction isolation level |

### Application Properties

**transfer-service/application.properties:**

```properties
# Server
server.port=8080

# Kafka Producer (with idempotence & transactions)
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5
spring.kafka.producer.transaction-id-prefix=transfer-service-${random.value}-

# Kafka Topics
withdraw-money-topic=withdraw-money-topic
deposit-money-topic=deposit-money-topic

# H2 Database
spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=test
spring.datasource.password=test

# JPA / Hibernate
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.h2.console.enabled=true
```

**deposit-service/application.properties:**

```properties
server.port=8081
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=deposit-group
spring.kafka.consumer.isolation-level=READ_COMMITTED
```

**withdrawal-service/application.properties:**

```properties
server.port=8082
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=withdrawal-group
spring.kafka.consumer.isolation-level=READ_COMMITTED
```

### Kafka Topics

Topics are automatically created by default (auto.create.topics.enable=true):

| Topic | Partitions | Replication | Purpose |
|-------|-----------|-------------|---------|
| `deposit-money-topic` | 3 | 1 | Deposit events |
| `withdraw-money-topic` | 3 | 1 | Withdrawal events |
| `deposit-money-topic.DLT` | 1 | 1 | Deposit DLQ |
| `withdraw-money-topic.DLT` | 1 | 1 | Withdrawal DLQ |

---

## Testing

### Unit Tests

```bash
# Test all modules
mvn test

# Test specific module
mvn test -pl transfer-service

# Test with coverage
mvn test jacoco:report
```

### Integration Tests (Embedded Kafka)

Spring Kafka provides `EmbeddedKafka` for testing without external broker:

```bash
mvn test -Dgroups=integration
```

### Manual Testing

**Test successful transfer:**

```bash
curl -X POST http://localhost:8080/transfers \
  -H "Content-Type: application/json" \
  -d '{
    "senderId": "alice",
    "recepientId": "bob",
    "amount": 100.00
  }'
```

**Check Kafka topics:**

```bash
# View deposit topic messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic deposit-money-topic --from-beginning

# View withdrawal topic messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic withdraw-money-topic --from-beginning
```

---

## Development

### Build Commands

```bash
# Build all modules
mvn clean install

# Build without tests
mvn clean install -DskipTests

# Build single module
mvn clean install -pl transfer-service

# Show dependency tree
mvn dependency:tree
```

### Running Services

**Terminal 1 — Kafka:**

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:latest
```

**Terminal 2 — Transfer Service:**

```bash
cd transfer-service && mvn spring-boot:run
```

**Terminal 3 — Deposit Service:**

```bash
cd deposit-service && mvn spring-boot:run
```

**Terminal 4 — Withdrawal Service:**

```bash
cd withdrawal-service && mvn spring-boot:run
```

### Debugging

Enable debug logging:

```properties
logging.level.org.springframework.kafka=DEBUG
logging.level.org.apache.kafka=DEBUG
```

View consumer group status:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group deposit-group --describe
```

---

## Architecture Decisions

### Why Kafka?

- **Decoupling:** Withdrawal and Deposit services run independently
- **Scalability:** Each service scales without affecting others
- **Reliability:** Dead-letter topics ensure no message loss
- **Ordering:** Partition-level ordering preserves transaction sequence per sender

### Why Transactional Semantics?

- **Consistency:** READ_COMMITTED prevents dirty reads during multi-step transfers
- **Correctness:** Ensures deposit never happens without corresponding withdrawal
- **Auditability:** All steps logged for compliance

### Why Spring Kafka?

- **Simplicity:** Declarative `@KafkaListener` with Spring annotations
- **Error Handling:** Built-in retry logic and dead-letter publishing
- **Integration:** Seamless Spring Boot integration with actuator health checks

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| Services can't connect to Kafka | Broker not running or wrong hostname | Verify `SPRING_KAFKA_BOOTSTRAP_SERVERS`, start Kafka broker |
| Messages stuck in DLT | Non-retryable exception occurred | Check service logs for root cause, fix, re-publish |
| Consumer lag increasing | Service crashing or processing slow | Scale replicas, check CPU/memory, review logs |
| "isolation level not supported" | Using uncommitted Kafka transactions | Ensure broker supports transactions, use READ_COMMITTED |
| MockService timeout (8083) | Port 8083 not listening | Start mock-service or change `callRemoteService()` URL |

### View Service Logs

```bash
# Transfer service logs
cd transfer-service && mvn spring-boot:run 2>&1 | grep -E "ERROR|WARN|Sent event"

# Kafka consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group deposit-group --describe

# Check DLT messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic deposit-money-topic.DLT --from-beginning
```

---

## Roadmap

### Completed ✓

- REST API for initiating transfers
- Multi-service Kafka consumer architecture
- Transactional semantics (READ_COMMITTED)
- Automatic retry with dead-letter fallback
- Mock service for validation

### Planned

- [ ] PostgreSQL database integration for account ledger
- [ ] Distributed tracing (Spring Cloud Sleuth + Jaeger)
- [ ] Prometheus metrics (throughput, latency, DLQ count)
- [ ] JWT authentication for Transfer API
- [ ] Transaction history API
- [ ] Idempotency key support for deduplication
- [ ] Kubernetes manifests / Helm charts
- [ ] Event sourcing for audit trail
- [ ] Compensation logic (saga pattern for failed transfers)

---

## Contributing

1. Create feature branch: `git checkout -b feature/your-feature`
2. Commit changes with caveman-style messages (terse, reason-focused)
3. Write tests for new logic
4. Ensure all tests pass: `mvn test`
5. Open pull request against `main`

**Code Style:**

- Follow existing patterns in handlers and services
- Add caveman comments to complex logic only
- Use Spring conventions (e.g., `@Transactional` for DB operations)

---

## Contact

- **Repository:** [github.com/toganbayev/kafka-transactions](https://github.com/toganbayev/kafka-transactions)
- **Author:** Daulet Toganbayev
- **Issues:** GitHub Issues

---

## License

TODO: Add license (MIT / Apache 2.0 / etc.)

---

## Acknowledgments

- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Transactions](https://kafka.apache.org/documentation/#semantics)
- [Spring Boot Starter Parent](https://spring.io/projects/spring-boot)
