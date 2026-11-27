CloudScale — Implementation Milestones

Objective

Build CloudScale incrementally as a reliable, observation, cloud-deployed distributed event processing platform.

Each milestone must produce a working, testable increment. Architecture and implementation decisions will evolve based on practical findings.


M0 - Project Foundation

Scope:
    - Java 21 and Spring Boot.
    - Maven multi-module project.
    - Git repository.
    - Docker-based PostgresSQL.
    - Flyway database migrations.
    - Initial ingestion service.

Acceptance criteria:
    - Application builds and starts successfully.
    - PostgresSQL connectivity is verified.
    - Database migrations are applied successfully.
    - Local development setup is reproducible.

M1 — Reliable Event Ingestion

Scope:
    - Event submission and retrieval REST APIs.
    - Request and response DTOs.
    - Input validation.
    - Global exception handling.
    - Request idempotency.
    - Unit and integration tests.

Acceptance criteria:
    - Valid events are durably persisted. 
    - Invalid requests return appropriate HTTP responses.
    - Duplicate requests do not create duplicate events.
    - Concurrent duplicate requests are handled safely.
    - Automated tests verify API behavior.

M2 — Transactional Outbox

Scope:
    - Outbox database schema.
    - Atomic event and outbox persistence.
    - Safe outbox record claiming.
    - Publication state management.

Acceptance criteria:
    -Event and outbox records commit atomically. 
    -Transaction failures do not leave partial records.
    -Concurrent workers cannot simultaneously claim the same pending record under normal claim ownership.

M3 — Kafka Integration

Scope:
    - Local Kafka infrastructure.
    - Topic and partition configuration.
    - Kafka producer. 
    - Outbox publication.
    - Broker acknowledgment handling.

Acceptance criteria:
    - Persisted events are published successfully.
    - Kafka unavailability does not silently lose accepted events.
    - Pending publications resume after recovery.

M4 — Asynchronous Processing

Scope:
    - Independent processing service. 
    - Kafka consumer groups.
    - Business processing logic.
    - Consumer-side idempotency.
    - Offset management.

Acceptance criteria:
    - Events are consumed and processed successfully.
    - Duplicate delivery does not repeat committed business effects.
    - Consumer restart does not cause silent event loss.
    - Processing correctness is verified through database assertions.

M5 — Failure Handling and Recovery

Scope:
    - Failure classification.
    - Bounded retries and backoff.
    - Dead-letter handling.
    - Controlled replay.
    - Failure-recovery experiments.

Acceptance criteria:
    - Temporary failures recover according to policy.
    - Permanently failing events remain recoverable.
    - Dead-letter replay preserves idempotency.
    - Publisher and consumer crash scenarios are tested.

M6 — Observability and Security

Scope:
    - Structured logging.
    - Spring Boot Actuator.
    - Micrometer metrics.
    - Application health monitoring.
    - API authentication and authorization.

Acceptance criteria:
    - Application health and processing behavior are observable.
    - Failures can be investigated using logs and metrics.
    - Protected endpoints enforce appropriate access controls.

M7 — CI/CD and Containerization

Scope:
    - GitHub Actions.
    - Automated build and test execution.
    - Static code analysis.
    - Application Docker images.
    - Container image publishing.

Acceptance criteria:
    - Automated checks execute reliably.
    - Failed mandatory checks prevent deployment.
    - Application containers can be built and started reproducibly.

M8 — AWS Deployment

Scope:
    - EC2 infrastructure.
    - Secure networking and IAM.
    - Persistent storage.
    - Container deployment.
    - CloudWatch monitoring.
    - Cost management.
    - Deployment automation.

Acceptance criteria:
    - CloudScale runs successfully on AWS.
    - Internal infrastructure is not publicly exposed.
    - Deployment and recovery procedures are documented.
    - Cloud resource utilization and costs are observable.

M9 — Performance and Scalability

Scope:
    - Reproducible synthetic event generation.
    - Load and stress testing.
    - Consumer scaling experiments.
    - Performance bottleneck analysis.
    - Failure testing under load.
    - Benchmark documentation.

Acceptance criteria:
    - At least 1,000,000 synthetic events are successfully processed on AWS.
    - Accepted and processed events are reconciled using stable event identifiers.
    - No unintended duplicate business effects occur.
    - No accepted events are silently lost during defined experiments.
    - Throughput, p95 and p99 latency, consumer lag, and resource utilization are measured.
    - Benchmark methodology and results are reproducible.

Milestone Management

Milestones may be refined, reordered, or divided into smaller increments as implementation progresses.

Testing, security, observability, and engineering quality are continuous responsibilities rather than activities restricted to individual milestones.

A milestone is complete only after its acceptance criteria have been verified.