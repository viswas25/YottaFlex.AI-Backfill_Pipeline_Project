## AI Tools Used

- Claude – Initial project implementation.
- ChatGPT – Architecture review, engineering improvements, code validation and documentation.

## 1. Requirement Analysis and Technical Design

**Objective:** Understand the business requirements and translate them into an implementation plan.

**Prompt:**

> Act as a Senior Data Engineer and Data Platform Architect
>
> Analyze the provided data engineering challenge requirements and identify the functional and non-functional requirements.
>
> Break the problem into ingestion, validation, transformation, loading, orchestration, tracking, recovery and reconciliation components.
>
> For each requirement, identify:
> - Expected system behavior.
> - Input and output data.
> - Processing rules.
> - Failure scenarios.
> - Validation approach.
>
> Create a requirement-to-component mapping. Do not introduce functionality that is not supported by the requirements.
>
> Prioritize correctness, maintainability, idempotency and recoverability.

## 2. Architecture and Code Quality Review

**Objective:** Review the existing implementation against the proposed design.

**Prompt:**

> Act as a Senior Data Engineer conducting a production-readiness code review.
>
> Review the existing Python data pipeline module by module.
>
> Evaluate separation of concerns, modularity, code readability, dependency management, error handling and database interactions.
>
> Identify unnecessary complexity, duplicated logic, hidden assumptions and potential failure points.
>
> Recommend changes only where they provide a clear engineering benefit.
>
> Preserve existing functionality unless a requirement or correctness issue justifies a change.

## 3. Data Quality and Validation

**Objective:** Strengthen data validation and ensure reliable processing.

**Prompt:**

> Act as a Senior Data Engineer specializing in data quality frameworks.
>
> Review the ingestion and validation implementation.
>
> Check schema validation, mandatory fields, data types, null handling, duplicate events, referential integrity, timestamp formats and unexpected schema changes.
>
> Identify missing edge cases and recommend targeted test cases.
>
> Ensure invalid records are handled consistently and valid records continue through the pipeline according to the challenge requirements.

## 4. Idempotency and Transaction Management

**Objective:** Ensure safe backfill execution and prevent duplicate data.

**Prompt:**

> Act as a Data Platform Engineer specializing in reliable batch processing.
>
> Review the partition loading implementation for idempotency and transaction safety.
>
> Evaluate how repeated executions, partial failures, transaction rollbacks and source file changes are handled.
>
> Verify that rerunning a successful partition does not create duplicate records or inconsistent current-state data.
>
> Identify potential race conditions and recommend practical improvements without introducing unnecessary infrastructure.

## 5. Retry, Restart and Recovery

**Objective:** Improve pipeline reliability.

**Prompt:**

> Act as a Senior Data Engineer responsible for production batch pipeline reliability.
>
> Review the orchestration, retry and restart mechanisms.
>
> Analyze worker failures, retry limits, partition status tracking, interrupted executions and recovery behavior.
>
> Recommend appropriate error handling and observability improvements.
>
> Ensure successfully completed partitions are not unnecessarily reprocessed and failed partitions can be recovered safely.

## 6. Reconciliation and Data Correctness

**Objective:** Validate source-to-target consistency.

**Prompt:**

> Act as a Senior Data Engineer responsible for data reconciliation.
>
> Review the reconciliation logic independently from the loading implementation.
>
> Validate source counts, accepted records, rejected records, duplicate handling, latest-event selection and target current-state calculations.
>
> Identify scenarios involving equal event timestamps, late-arriving records and multiple versions of the same business entity.
>
> Recommend independent validation queries and automated test cases.

## 7. Testing and Edge Case Analysis

**Objective:** Improve confidence in the implementation.

**Prompt:**

> Act as a Senior Python Engineer and QA reviewer.
>
> Review the existing Pytest suite and identify gaps in functional, integration and failure-recovery testing.
>
> Cover empty files, malformed records, missing columns, duplicate events, invalid references, transaction failures, retry behavior, restart behavior and repeated execution.
>
> Recommend focused tests that validate observable outcomes rather than implementation details.
>
> Do not claim a test has passed unless it has actually been executed.

## 8. Performance and Scalability Review

**Objective:** Evaluate performance and identify bottlenecks.

**Prompt:**

> Act as a Senior Data Platform Architect.
>
> Analyze the pipeline's performance characteristics, including CSV reading, memory consumption, parallel partition processing, database write throughput and reconciliation queries.
>
> Identify potential bottlenecks and recommend optimizations appropriate for the current SQLite-based architecture.
>
> Explain the trade-offs between parallelism, database write contention, batch size and memory usage.
>
> Distinguish measured benchmark results from theoretical performance estimates.

## 9. Documentation and Maintainability

**Objective:** Improve project documentation.

**Prompt:**

> Act as a Senior Software Engineer reviewing a data engineering project before public GitHub submission.
>
> Review the README, architecture documentation, setup instructions, data dictionary and operational walkthrough.
>
> Ensure the documentation accurately reflects the implemented code.
>
> Remove unsupported claims, outdated execution commands and unnecessary technical jargon.
>
> Explain the architecture, design decisions, assumptions, limitations and verification steps clearly.

## 10. Final Submission Review

**Objective:** Perform a final engineering review.

**Prompt:**

> Act as a technical interviewer evaluating an Associate AI Forward Deployed Engineer candidate's data engineering submission.
>
> Compare the implementation against the original challenge requirements and evaluation criteria.
>
> Review correctness, architecture, code quality, testing, idempotency, recovery, observability, performance and documentation.
>
> Identify critical defects, incomplete requirements and interview discussion points.
>
> Provide a prioritized pre-submission checklist.
>
> Do not invent test results, implementation details or personal contributions.

## AI Usage and Validation

AI tools were used to support the initial implementation, technical analysis, code review, debugging assistance and documentation.

The implementation is being reviewed against the original project requirements. Validation includes:

- Local execution of the complete pipeline.
- Automated testing using Pytest.
- Source-to-target data reconciliation.
- Verification of retry, restart and idempotency behavior.
- Manual review of code, assumptions and design decisions.

All AI-assisted development activities and prompts are documented in this repository. Final test results and validation evidence will reflect actual execution.