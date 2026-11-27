CloudScale — Engineering Standards

1. Development Principles

   - Prefer simplicity over unnecessary complexity.
   - Introduce technologies to solve identifiable engineering problems.
   - Maintain clear service boundaries and data ownership.
   - Verify architectural assumptions through practical experiments.
   - Document significant decisions and trade-offs.

2. Code Quality

   - Use Java 21 and established Spring Boot conventions.
   - Separate API, business logic, and persistence responsibilities.
   - Use DTOs for public API contracts.
   - Maintain explicit transaction boundaries. 
   - Implement consistent validation and exception handling.
   - Externalize environment-specific configuration.
   - Avoid unnecessary abstraction and dependencies.

3. Testing Standards

    The project will incrementally introduce:

    - JUnit and Mockito for unit testing.
    - Spring Boot integration testing.
    - Testcontainers for PostgreSQL and Kafka integration tests.
    - API contract and validation testing.
    - Concurrency and idempotency testing.
    - Failure recovery testing.
    - Performance and load testing.

    Critical reliability guarantees must be supported by automated tests.

4. Database Standards

   - Manage schema changes through Flyway migrations.
   - Do not modify previously applied migrations.
   - Enforce critical data integrity rules at the database level.
   - Introduce indexes based on query requirements and measured performance.
   - Respect service-level data ownership.

5. Git Workflow

   - Maintain a stable main branch. 
   - Use feature branches for significant changes. 
   - Make small, meaningful commits. 
   - Use descriptive commit messages. 
   - Run relevant tests before merging.
   - Never commit credentials, secrets, or generated build artifacts.

6. Continuous Integration and Deployment

   The CI/CD pipeline will incrementally support:

   - Compilation and automated testing.
   - Static code analysis.
   - Container image creation.
   - Container image publishing. 
   - Deployment and health verification.

   Failed mandatory checks must prevent automated deployment.

7. Security

   - Follow least-privilege principles. 
   - Store secrets outside source control. 
   - Validate external inputs. 
   - Secure application and infrastructure access. 
   - Avoid exposing sensitive information in logs and API responses.

8. AI-Assisted Development

 AI-generated code must be reviewed, understood, and verified before acceptance.
 
 Architectural decisions, implementation correctness, and security remain the responsibility of the developer.
 
9. Definitions of Done

    A feature is considered done when:

    - It meets the acceptance criteria.
    - The implementation compiles successfully.
    - Relevant automated tests pass.
    - Application behavior is verified.
    - Relevant failure scenarios are addressed.
    - Documentation is updated where necessary.
    - Changes are committed to version control.