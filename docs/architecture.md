# CloudScale — System Architecture

**Version:** 1.0 (Draft)  
**Status:** In Design

## 1. Overview

CloudScale is a distributed, event-driven processing platform built using Java 21, Spring Boot, Apache Kafka, PostgreSQL, Docker, and AWS.

The platform accepts business events through REST APIs, persists them durably, publishes them to Kafka, and processes them asynchronously through independently scalable consumers.

CloudScale is designed to demonstrate reliable event processing, fault tolerance, horizontal scalability, cloud deployment, and production-oriented engineering practices.

## 2. Project Goals

1. Build a reliable event ingestion and processing platform.
2. Prevent duplicate business effects during retries and Kafka redelivery.
3. Support asynchronous processing through Kafka partitions and consumer groups.
4. Recover from temporary failures without silently losing accepted events.
5. Implement bounded retries, dead-letter handling, and controlled event replay.
6. Deploy the platform on AWS EC2.
7. Implement automated testing, CI/CD, and observability.
8. Successfully process **1,000,000+ synthetic events** in a reproducible benchmark.
9. Measure throughput, p95/p99 latency, and recovery behavior.

## 3. Core Architecture Decision

### ADR-001: Synchronous Ingestion, Asynchronous Processing

CloudScale will synchronously validate and persist an incoming event and its corresponding outbox record in a single PostgreSQL transaction.

After the transaction commits, the API returns `201 Created` with the event identifier.

A separate outbox publisher asynchronously publishes pending events to Kafka. Consumer services process events independently.

**Benefits**
- Durable event acceptance before responding to clients.
- Decoupling of ingestion from downstream processing.
- Recovery from temporary Kafka outages.
- Independent scaling of event ingestion and consumers.

**Tradeoffs**
- Eventual consistency between ingestion and processing.
- Additional complexity from the transactional outbox.
- Need for idempotent publishing and consumer processing.

## 4. Implementation Status

This document describes the target architecture. Features are not considered implemented until verified through automated tests and working demonstrations.

## 5. Service Boundaries and Responsibilities

### 5.1 Ingestion Service

The ingestion service provides REST APIs for accepting and retrieving events.

Responsibilities:

- Validate incoming event requests.
- Enforce request idempotency.
- Persist events and corresponding outbox records atomically.
- Return an event identifier after successful persistence.
- Expose event status retrieval APIs.

The ingestion service does not wait for downstream Kafka consumers to complete processing.

### 5.2 Outbox Publisher

The outbox publisher transfers committed events from PostgreSQL to Kafka.

Responsibilities:

- Read unpublished outbox records.
- Publish events to the appropriate Kafka topic.
- Mark records as published after receiving broker acknowledgment.
- Retry publishing after temporary failures.
- Avoid concurrent workers claiming the same pending record.

The publisher may initially run as a background component within the ingestion service. Independent deployment remains an option if scaling or operational requirements justify it.

Publishing provides at-least-once delivery semantics. Consumers must tolerate duplicate messages.

### 5.3 Kafka

Apache Kafka acts as the asynchronous event distribution layer.

Responsibilities:

- Store events in partitioned topics according to configured retention.
- Distribute records among consumers in a consumer group.
- Support replay and independent consumer scaling.

Partition keys, partition counts, retention settings, and topic naming will be finalized in the Kafka design section.

### 5.4 Processing Service

The processing service consumes Kafka events and performs business processing.

Responsibilities:

- Consume records using Kafka consumer groups.
- Apply event-specific processing logic.
- Prevent duplicate business effects during redelivery.
- Commit offsets only after successful processing or durable failure handoff.
- Support bounded retries and dead-letter recovery.

Processing instances can be scaled independently from the ingestion API.

### 5.5 Data Ownership

The ingestion service owns event ingestion records and the transactional outbox.

The processing service owns its processing results and consumer idempotency records.

Services must not directly modify each other's owned tables. Separate databases or schemas may be introduced to enforce ownership boundaries.

### 5.6 Deployment Strategy

Initially, CloudScale will run locally using Docker Compose.

The first AWS deployment will use EC2 and containerized services.

Separate deployments and horizontal scaling will be introduced as required by performance and reliability experiments.

## 6. Event Lifecycle and Failure Handling

### 6.1 Normal Event Lifecycle

1. A client submits a business event through `POST /api/v1/events`.
2. The ingestion service validates the event and checks its idempotency key.
3. Within a single PostgreSQL transaction, the ingestion service persists the event and its corresponding outbox record.
4. After the transaction commits, the API returns `201 Created` with the event identifier.
5. The outbox publisher reads pending records and publishes them to Kafka.
6. After Kafka acknowledges successful publication, the publisher marks the outbox record as published.
7. A processing service consumes the Kafka record and executes the corresponding business operation.
8. The processing service durably records its result and consumer idempotency information.
9. After successful processing, the consumer commits its Kafka offset.
10. Processing status is made available through a status projection or another explicitly defined mechanism.

### 6.2 Failure Scenarios

**Database failure during ingestion**

If PostgreSQL is unavailable or the transaction fails, the event and outbox record are not committed. The API must not return a successful creation response.

**Kafka unavailable**

If Kafka cannot accept events, the publisher retains pending outbox records and retries publication. Previously accepted events remain stored in PostgreSQL.

**Publisher crash after Kafka acknowledgment**

If Kafka accepts a record but the publisher crashes before marking the outbox record as published, the event may be published again. Consumers must identify duplicates using stable event identifiers.

**Consumer crash before offset commit**

Kafka may redeliver a record after the consumer restarts. Consumer processing must be idempotent so repeated delivery does not create duplicate business effects.

**Temporary processing failure**

The processing service performs bounded retries with backoff. Retry behavior must not block unrelated events indefinitely.

**Permanent processing failure**

After retry limits are exhausted, the failed event is transferred to durable dead-letter handling. Authorized replay can reintroduce the event after investigation or correction.

### 6.3 Delivery Guarantees

CloudScale targets at-least-once event delivery with idempotent business effects.

Exactly-once end-to-end processing is not assumed.

Correctness depends on durable persistence, unique event identifiers, transactional processing boundaries, deduplication, and safe Kafka offset handling.

### 6.4 Event Status

The target event lifecycle includes:

- `RECEIVED` — Event durably accepted.
- `PUBLISHED` — Kafka publication acknowledged.
- `PROCESSING` — Consumer processing underway.
- `COMPLETED` — Business processing completed successfully.
- `FAILED` — Processing could not complete successfully.

These are logical lifecycle states, not yet a finalized database state machine.

Because ingestion and processing components own different data, status synchronization must use an explicitly designed mechanism rather than direct cross-service table updates.

### 6.5 Verification Requirements

Automated integration and failure-recovery tests must demonstrate:

- No partial event/outbox commits.
- Recovery after temporary Kafka unavailability.
- Safe handling of duplicate Kafka delivery.
- Consumer recovery after restart.
- Durable handling of permanently failing events.
- Correct final processing counts after retries and redelivery.

## 7. Database Design

### 7.1 Overview

CloudScale uses PostgreSQL for durable event ingestion, transactional outbox publishing, and consumer-side idempotency.

Database ownership is separated by service responsibility.

### 7.2 Events Table

The `events` table stores the original event accepted through the ingestion API.

Core fields:

| Column         | Purpose                                                 |
|----------------|---------------------------------------------------------|
| `id`           | Unique event identifier (UUID primary key)              |
| `event_type`   | Business event type                                     |
| `payload`      | Event content stored as JSONB                           |
| `status`       | Ingestion-owned event lifecycle state                   |
| `created_at`   | Event creation timestamp                                |
| `processed_at` | Existing field; ownership and future use to be reviewed |

The existing Flyway V1 migration creates this table.

Request idempotency will be enforced using a stable client-provided idempotency key, scoped to the producing client, with a database uniqueness constraint.

The exact producer identity and idempotency schema will be finalized before implementing the API.

### 7.3 Transactional Outbox Table

The `outbox_events` table records events that must be published to Kafka.

Proposed fields:

| Column          | Purpose                          |
|-----------------|----------------------------------|
| `id`            | Unique outbox record identifier  |
| `event_id`      | Reference to the original event  |
| `topic`         | Destination Kafka topic          |
| `partition_key` | Stable Kafka partitioning key    |
| `payload`       | Serialized event message         |
| `status`        | Publication state                |
| `attempt_count` | Number of publication attempts   |
| `created_at`    | Outbox creation timestamp        |
| `published_at`  | Successful publication timestamp |

The event record and outbox record must be inserted within the same database transaction.

The publisher will claim pending records safely, publish them to Kafka, and update publication status only after broker acknowledgment.

Multiple publisher instances must not claim the same pending record concurrently. PostgreSQL row locking or an equivalent claim mechanism will be evaluated.

A publisher crash after Kafka acknowledgment can cause duplicate publication. Therefore, outbox publishing provides at-least-once delivery rather than exactly-once delivery.

### 7.4 Consumer Deduplication Table

The processing service maintains a `processed_events` table within its own data ownership boundary.

Proposed fields:

| Column | Purpose |
|---|---|
| `consumer_name` | Logical consumer identity |
| `event_id` | Stable event identifier |
| `processed_at` | Successful processing timestamp |

A composite primary key or unique constraint on `(consumer_name, event_id)` prevents duplicate processing records for the same logical consumer.

The consumer must persist its deduplication record and corresponding business changes atomically.

A duplicate event must not repeat already committed business effects.

### 7.5 Transaction Boundaries

**Ingestion transaction**

1. Validate the request and enforce idempotency.
2. Insert the event.
3. Insert the corresponding outbox record.
4. Commit both records atomically.

**Consumer transaction**

1. Identify the incoming event.
2. Check or claim its deduplication identity.
3. Apply the business operation.
4. Persist the processing result and deduplication record.
5. Commit the database transaction.
6. Commit the Kafka offset after successful database commit.

The consumer must handle concurrent duplicate deliveries safely using database-enforced uniqueness, not only application-level existence checks.

### 7.6 Indexing and Retention

Initial indexing will support:

- Event lookup by identifier.
- Idempotency-key uniqueness.
- Efficient selection of pending outbox records.
- Consumer deduplication lookups.

Retention and cleanup policies for events, published outbox records, and deduplication records must be defined before production-style benchmarking.

Deduplication records must not be deleted while corresponding events can still be replayed without an alternative duplicate-protection mechanism.

### 7.7 Migration Strategy

All schema changes will use versioned Flyway migrations.

The existing `V1__create_events_table.sql` migration must not be edited after application.

Future changes will use new migrations such as V2 and V3.

No new database migration will be implemented until the relevant schema decisions are approved.

## 8. Kafka Design

### 8.1 Purpose

Apache Kafka provides durable, asynchronous event distribution between the outbox publisher and processing services.

CloudScale targets at-least-once delivery with idempotent consumer-side business effects.

### 8.2 Topic Design

Initial proposed topics:

| Topic | Purpose |
|---|---|
| `cloudscale.events.v1` | Main business event stream |
| `cloudscale.events.retry.v1` | Events awaiting retry |
| `cloudscale.events.dlq.v1` | Events requiring investigation or controlled replay |

Topic names and retry routing are initial design proposals.

The main topic will initially use 12 partitions for scalability experiments. Partition count and replication factor will be configurable by environment.

Local development may use a single Kafka broker. AWS deployment will document the availability limitations of the chosen broker topology.

### 8.3 Partitioning Strategy

Events will be published using a stable business partition key, such as `orderId` for order-related events.

Events with the same key should map to the same partition while the topic partition count remains unchanged.

This supports ordered consumption for related business events.

CloudScale will not assume global ordering across Kafka partitions.

Increasing a topic's partition count may change the partition assignment of future events with the same key. Any ordering-sensitive workflow must account for this behavior.

### 8.4 Consumer Groups

Processing service instances will share a Kafka consumer group.

Kafka assigns partitions among consumers within the group.

The initial benchmark will compare configurations with:

- 1 consumer
- 3 consumers
- 6 consumers
- 12 consumers

With 12 partitions, additional consumers beyond the number of partitions do not increase partition-level parallelism within the same consumer group.

Consumer scaling results must be measured rather than assumed.

### 8.5 Offset Management

Consumer offset management will prioritize correctness over convenience.

The consumer must not acknowledge successful processing before the required database transaction commits.

If a consumer crashes after committing database changes but before committing its Kafka offset, the record may be delivered again.

Consumer-side idempotency must prevent repeated business effects.

Offset commits and retry/DLQ handoff require explicit failure handling. A failed record must not be skipped unless its recovery path has been durably established.

### 8.6 Retry and Dead-Letter Strategy

Temporary failures will use bounded retries with backoff.

Events that cannot be processed within the retry policy will be transferred to dead-letter handling.

The retry design must consider:

- Ordering requirements.
- Retry attempt tracking.
- Consumer throughput.
- Avoiding indefinite partition blocking.
- Durable handoff before offset advancement.

The final retry mechanism will be selected during detailed consumer design.

### 8.7 Message Contract

Kafka messages will include:

- Stable event identifier.
- Event type.
- Schema version.
- Event creation timestamp.
- Producer or correlation metadata where appropriate.
- Business payload.

Messages will use a versioned contract to support controlled schema evolution.

### 8.8 Performance Benchmark

CloudScale must successfully process at least **1,000,000 synthetic events** in a reproducible benchmark.

The benchmark will record:

- Number of generated, accepted, published, and successfully processed events.
- Throughput in events per second.
- p95 and p99 latency with clearly defined measurement boundaries.
- Kafka consumer lag.
- Consumer scaling behavior.
- Duplicate delivery and duplicate business-effect counts.
- Error rates and recovery behavior.
- Hardware, event size, partition count, and consumer configuration.

A benchmark is considered successful only when processing correctness is verified, not merely when one million messages have been produced.

Benchmark results will be published separately from architecture targets.

## 9. Reliability, Idempotency, and Recovery

### 9.1 Reliability Objectives

CloudScale must tolerate temporary infrastructure failures, duplicate requests, duplicate message delivery, and unexpected application restarts.

The platform must not silently lose durably accepted events during the defined failure scenarios.

Reliability guarantees must be verified through automated integration tests and controlled failure experiments.

### 9.2 Request Idempotency

Clients will provide an `Idempotency-Key` HTTP header when submitting events.

The ingestion service will enforce uniqueness using the combination of producer identity and idempotency key.

Expected behavior:

- A new idempotency key creates a new event.
- Repeating the same key with an equivalent request returns the existing event identifier.
- Reusing the same key with a different request payload returns `409 Conflict`.
- Concurrent requests using the same key must not create multiple events.

Database uniqueness constraints will provide the final protection against concurrent duplicate requests.

Request equivalence and idempotency-key retention policies will be finalized during API design.

### 9.3 Reliable Event Publication

The ingestion service will persist the event and its corresponding outbox record within a single PostgreSQL transaction.

The outbox publisher will:

- Claim pending records safely.
- Publish events to Kafka.
- Wait for broker acknowledgment.
- Mark acknowledged records as published.
- Retry unsuccessful publication attempts with backoff.
- Recover abandoned claims after publisher failure.

Unpublished records must remain recoverable after application restarts.

Duplicate publication remains possible and must be handled by consumers.

### 9.4 Consumer Idempotency

Every Kafka message must contain a stable event identifier.

The processing service will enforce uniqueness using the combination of logical consumer identity and event identifier.

The consumer deduplication record and corresponding business database changes must commit within the same transaction.

Kafka offsets must be committed only after successful database processing or durable failure handoff.

Repeated delivery of an already processed event must not repeat committed business effects.

External side effects, such as payment APIs or email delivery, require separate idempotency guarantees and are not automatically protected by a local database transaction.

### 9.5 Retry Strategy

Failures will be classified as transient or permanent.

Transient failures may include:

- Temporary database connectivity issues.
- Network timeouts.
- Temporary downstream service unavailability.

Permanent failures may include:

- Unsupported event types.
- Invalid message contracts.
- Unrecoverable business validation errors.

Consumer retries will use bounded attempts and exponential backoff with jitter.

Retry limits and delays will be configurable.

The outbox publisher will retain unsuccessful publications for recovery rather than silently discard accepted events.

### 9.6 Dead-Letter Handling

Events that cannot be processed successfully within the consumer retry policy will be transferred to a dead-letter topic.

Dead-letter records should retain:

- Original event identifier and payload.
- Failure category.
- Error details.
- Retry attempt count.
- Failure timestamp.
- Original topic, partition, and offset.

The consumer must not advance its offset past a failed record unless the required recovery handoff has been durably established.

### 9.7 Controlled Event Replay

CloudScale will support controlled replay of failed events after investigation or correction.

Replay must:

- Preserve the original event identifier.
- Maintain consumer idempotency guarantees.
- Record replay attempts for auditing.
- Prevent uncontrolled retry loops.
- Require appropriate authorization.

Replay must not repeat business effects that were already committed successfully.

### 9.8 Failure-Recovery Testing

CloudScale must demonstrate recovery from the following scenarios:

| Scenario                                                      | Expected Result                                  |
|---------------------------------------------------------------|--------------------------------------------------|
| Duplicate API requests                                        | Only one event is created                        |
| Concurrent duplicate requests                                 | Database uniqueness prevents duplicate creation  |
| Kafka broker unavailable                                      | Accepted events remain recoverable in the outbox |
| Publisher restart                                             | Pending publications resume                      |
| Duplicate Kafka delivery                                      | No duplicate business database effects           |
| Consumer crash before database commit                         | Processing can safely retry                      |
| Consumer crash after database commit but before offset commit | Redelivery does not repeat committed effects     |
| Temporary processing failure                                  | Processing retries according to policy           |
| Permanent processing failure                                  | Event is transferred to dead-letter handling     |
| Dead-letter replay                                            | Corrected event can be processed safely          |

### 9.9 Reliability Acceptance Criteria

- Zero unintended duplicate business database effects during controlled tests.
- Zero silently lost accepted events during defined failure experiments.
- Successful recovery following temporary infrastructure outages.
- Verifiable dead-letter handling and replay.
- Measured recovery time following consumer and publisher restarts.
- Verified processing correctness during the mandatory 1,000,000+ event benchmark.

### 9.10 Known Limitations

CloudScale does not claim exactly-once end-to-end delivery.

Infrastructure durability depends on PostgreSQL configuration, Kafka replication, persistent storage, and the deployed environment.

A single-node deployment does not provide high availability against complete node or storage failure.

These limitations must be documented alongside benchmark and recovery results.

## 10. API Contracts and Security

### 10.1 REST API Design

CloudScale exposes versioned REST APIs using the `/api/v1` prefix.

Initial endpoints:

| Method | Endpoint                   | Purpose                           |
|--------|----------------------------|-----------------------------------|
| POST   | `/api/v1/events`           | Submit a business event           |
| GET    | `/api/v1/events/{eventId}` | Retrieve event details and status |

Administrative endpoints for dead-letter investigation and controlled replay will be introduced when the corresponding capabilities are implemented.

### 10.2 API Standards

All REST APIs will follow consistent standards for:

- Request and response DTOs.
- Input validation.
- Appropriate HTTP status codes.
- Structured error responses.
- Request idempotency.
- Correlation identifiers.
- API versioning.
- OpenAPI documentation.

Persistence entities must not be exposed directly as public API response contracts.

### 10.3 Request Idempotency

Event submission will require an `Idempotency-Key` header.

Idempotency keys will be scoped to the producing client.

Equivalent repeated requests must return the existing event identifier without creating duplicate events.

Reusing an idempotency key with different request content must return `409 Conflict`.

### 10.4 Security

CloudScale will adopt the following security principles:

- Authentication and authorization for protected APIs.
- Least-privilege access to infrastructure and application resources.
- HTTPS for externally accessible APIs.
- Secure management of credentials and secrets.
- Restricted access to PostgreSQL and Kafka.
- Authorization and auditing for administrative operations.
- Avoidance of sensitive information in application logs.

Credentials must not be committed to source control.

### 10.5 API Protection

The platform will consider:

- Request payload size limits.
- Rate limiting.
- Input validation.
- Request timeouts.
- Protection against resource exhaustion.

Specific limits will be established through implementation and performance testing.

### 10.6 Implementation Approach

Security and API protection will be introduced incrementally.

The initial local implementation will prioritize event ingestion correctness, followed by authentication, authorization, and deployment security.

The final authentication mechanism and infrastructure-specific security configuration will be selected during implementation.

Public AWS deployment must not expose unsecured application APIs or internal infrastructure services.

## 11. AWS Deployment Architecture

### 11.1 Deployment Objectives

CloudScale will be deployed on AWS to demonstrate practical cloud infrastructure management, containerized application deployment, secure networking, observability, and performance benchmarking.

The initial deployment will prioritize simplicity, reproducibility, and cost control.

### 11.2 Initial Deployment

The first AWS deployment will use an EC2 Linux instance running Docker-based containers.

Initial components:

- Ingestion service.
- Outbox publisher.
- Processing service.
- Apache Kafka.
- PostgreSQL.

Persistent storage will be configured for stateful infrastructure components.

The initial deployment may host these components on a single EC2 instance.

### 11.3 AWS Infrastructure

The platform will incrementally utilize:

- EC2 for application hosting.
- VPC for network isolation.
- Security Groups for network access control.
- IAM for least-privilege AWS access.
- EBS for persistent storage.
- ECR for container image storage.
- CloudWatch for monitoring and logging.
- S3 for benchmark reports and selected artifacts.
- AWS Budgets for cost monitoring.

### 11.4 Deployment Security

- PostgreSQL and Kafka must not be publicly accessible.
- Administrative access must be restricted.
- Public application endpoints must use HTTPS and appropriate authentication.
- Credentials and secrets must not be embedded in source code or container images.
- AWS resource access must follow least-privilege principles.

### 11.5 Scaling Strategy

The ingestion and processing services must support independent scaling.

Initial scaling experiments will evaluate multiple application instances and Kafka consumers.

Multi-instance EC2 deployments may be introduced to evaluate horizontal scalability and resource isolation.

Managed services such as ECS, RDS, and MSK may be evaluated later if justified by operational requirements.

### 11.6 Deployment Automation

Deployment will evolve from an initial manual deployment to a reproducible automated process.

The target workflow includes:

- Automated application builds and tests.
- Docker image creation.
- Container image publishing.
- Automated deployment.
- Application health verification.
- Documented rollback procedures.

### 11.7 Cloud Benchmark

The mandatory 1,000,000+ event benchmark must be executed on AWS infrastructure.

Benchmark results must include infrastructure specifications, application configuration, processing correctness, throughput, latency, resource utilization, and estimated infrastructure cost.

### 11.8 Cost Management

AWS resources must be provisioned according to actual project requirements.

Cost monitoring, budget alerts, and resource cleanup procedures will be established.

Infrastructure that is not required must be stopped or removed when appropriate.

### 11.9 Known Limitations

The initial single-instance deployment does not provide infrastructure-level high availability.

Multiple containers running on the same EC2 instance share CPU, memory, storage, and infrastructure failure risks.

Production-grade availability and disaster recovery require additional infrastructure and explicit design decisions.

Exact instance types, storage capacity, networking topology, and deployment configurations will be finalized during implementation.

## 12. Observability and Performance

### 12.1 Observability Objectives

CloudScale must provide sufficient operational visibility to monitor application health, investigate failures, identify performance bottlenecks, and verify processing correctness.

Observability will be introduced incrementally through structured logging, metrics, and distributed tracing.

### 12.2 Structured Logging

Application logs should include relevant contextual information:

- Timestamp.
- Service name.
- Log level.
- Event identifier.
- Correlation identifier.
- Operation name.
- Error category and details.

Logs must not expose credentials or sensitive payload information.

### 12.3 Application Metrics

CloudScale will monitor:

- API request count, latency, and error rate.
- Accepted event count.
- Pending outbox record count and age.
- Successful and failed Kafka publications.
- Kafka consumer lag.
- Successfully processed event count.
- Duplicate event detection count.
- Retry and dead-letter counts.
- Database connection pool utilization.
- JVM memory and garbage collection.
- Infrastructure resource utilization.

### 12.4 Health Monitoring

Spring Boot Actuator will provide application health and operational endpoints.

Health checks will distinguish application liveness from readiness to serve requests.

Health and management endpoints must be appropriately secured.

### 12.5 Monitoring Infrastructure

The initial observability implementation will use:

- Spring Boot Actuator for application health.
- Micrometer for application metrics.
- Structured application logging.
- AWS CloudWatch for cloud infrastructure monitoring and selected application telemetry.

Distributed tracing using OpenTelemetry and additional visualization tools may be introduced as the platform evolves.

### 12.6 Performance Benchmarking

CloudScale must successfully process at least 1,000,000 synthetic events in a reproducible AWS benchmark.

Benchmark measurements will include:

- API ingestion throughput.
- End-to-end processing throughput.
- API response latency.
- End-to-end event processing latency.
- p50, p95, and p99 latency.
- Kafka consumer lag.
- CPU, memory, disk, and network utilization.
- Error and retry rates.
- Duplicate business-effect counts.
- Recovery time following controlled failures.

### 12.7 Scalability Experiments

Performance experiments will evaluate:

- Increasing consumer concurrency.
- Different Kafka partition configurations.
- Outbox publisher batch sizes.
- Database connection pool configurations.
- Multiple application instances.
- Infrastructure resource allocation.

Experiments should change one primary variable at a time where practical.

Performance improvements must be supported by measured results.

### 12.8 Benchmark Correctness

Benchmark validation must reconcile generated, accepted, published, processed, and failed events using stable event identifiers.

Duplicate message deliveries must be distinguished from duplicate business effects.

A benchmark is successful only when processing correctness is verified alongside performance.

### 12.9 Performance Targets

Initial performance baselines will be established through practical testing.

Throughput targets, latency thresholds, alerting thresholds, and service-level objectives will be refined based on measured behavior and infrastructure constraints.

Unverified performance claims must not be presented as achieved results.

### 12.10 Documentation

Benchmark methodology, environment configuration, measured results, identified bottlenecks, and optimization decisions will be maintained separately from the architecture document.

Operational recovery procedures will be documented as the platform evolves.