# CloudScale

A distributed event processing platform built with Java, Spring Boot, PostgreSQL, Apache Kafka, and Docker.

## Project Overview

CloudScale is a hands-on backend engineering project designed to explore event-driven architecture, reliable message processing, database transactions, and distributed systems.

The platform will accept events through REST APIs, persist them in PostgreSQL, and process them asynchronously using Apache Kafka.

## Technology Stack

- **Java 21** — Application development target
- **Spring Boot 4** — Backend services and REST APIs
- **PostgreSQL 16** — Relational database and event persistence
- **Flyway** — Version-controlled database migrations
- **Apache Kafka** — Planned asynchronous event streaming
- **Docker Compose** — Local infrastructure
- **Maven** — Multi-module build management

## Architecture

Planned event processing flow:

```text
Client
  |
  v
Ingestion Service (Spring Boot)
  |
  v
PostgreSQL (Event Persistence)
  |
  v
Kafka (Event Streaming)
  |
  v
Processing Service
  |
  v
Event Status Updates
```

The architecture will evolve as Kafka integration and additional services are implemented.

## Project Structure

```text
cloudscale/
├── .mvn/
├── infra/
│   └── docker/
│       └── compose.yaml
├── ingestion-service/
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/
│       │   └── resources/
│       │       ├── application.yaml
│       │       └── db/migration/
│       └── test/
├── mvnw
├── mvnw.cmd
└── pom.xml
```


## Running Locally

### Prerequisites

- Java 21 or compatible JDK
- Docker Desktop
- Git

### Start PostgreSQL

```bash
export POSTGRES_PASSWORD='your_local_password'
docker compose -f infra/docker/compose.yaml up -d
```

The database is available on `localhost:5434`.

### Start the ingestion service

```bash
./mvnw -pl ingestion-service spring-boot:run
```

The ingestion service starts on `http://localhost:8080`.

### Check application health

```bash
curl http://localhost:8080/actuator/health
```

Expected response:

```json
{"status":"UP"}
```

## Development Roadmap

1. Implement event ingestion and persistence.
2. Add request validation and integration tests.
3. Integrate Apache Kafka.
4. Build an asynchronous event processing service.
5. Implement idempotency, retries, and failure recovery.
6. Add observability and CI/CD.
7. Deploy the platform to AWS.

## Purpose

CloudScale demonstrates practical backend engineering concepts through incremental implementation, testing, and deployment.