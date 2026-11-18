# Implement Background Jobs in Code

## Project overview
This project implements an asynchronous background-job workflow for invoice PDF generation and customer delivery. The API accepts an invoice request, creates a job, queues it, and immediately returns a job ID. A separate worker later generates the PDF and invokes a delivery adapter.

## Assessment tasks

### Task 1 — Workflow contract and state model
- Defined invoice input, job status, event, result, trace, and background-job models.
- Implemented `queued`, `running`, `completed`, and `failed` states with legal transitions.
- Documented the state-transition contract and tested valid and invalid transitions.

### Task 2 — Background Jobs implementation
- Implemented an in-memory job store and FIFO queue.
- Added an API boundary that returns a job ID without waiting for worker execution.
- Implemented a separate worker, invoice PDF generation with ReportLab, and a delivery-adapter protocol.
- Used a fake delivery adapter for repeatable local testing.

### Task 3 — Failure, retry, and replay safety
- Added an idempotency ledger and duplicate-enqueue protection.
- Classified transient timeout/connection failures separately from permanent failures.
- Implemented bounded retries and cached-result replay for completed jobs.

### Task 4 — Integration boundary and observability
- Added delivery call, success, failure, success-rate, and latency metrics.
- Added read-only status lookup, aggregate job metrics, events, and execution traces.
- Exported API acceptance, pre-processing status, worker result, post-processing status, job count, metrics, and traces.
- Added recursive sensitive-field redaction.

### Task 5 — Operator runbook and evidence report
- Generated an operator runbook covering normal processing, failure recovery, replay protection, and observability.
- Validated Task 2 and Task 4 JSON evidence.
- Checked both generated invoice PDFs using their PDF signatures.
- Produced a consolidated evidence report.

## Project structure
```text
Implement_Background_Jobs/
├── background_jobs.py
├── background_job_engine.py
├── background_job_reliability.py
├── background_job_observability.py
├── background_job_runbook.py
├── tests/
├── docs/
├── artifacts/
├── outputs/
├── traces/
└── reports/
```

## Environment and setup
Tested with Python 3.13, Pydantic 2.x, pytest 9.x, and ReportLab 4.x.

```cmd
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

## Run the demonstrations
Run these commands from the project root:

```cmd
python background_jobs.py
python background_job_engine.py
python background_job_reliability.py
python background_job_observability.py
python background_job_runbook.py
```

Run all tests:

```cmd
python -m pytest -q
```

## Verified results
Final verification on 18 November 2026:
- **37 tests passed** (`python -m pytest -q`).
- **4 of 4 evidence artifacts valid**.
- `evidence_complete` reported `true`.
- Task 4 showed immediate API acceptance with `queued` status, queued status before worker execution, successful worker completion, and final `completed` status.
- Both Task 2 and Task 4 invoice PDFs passed the PDF signature check.

Evidence is written to `outputs/task2_background_job.json`, `outputs/task4_observability.json`, and `reports/evidence_report.json`. The operator runbook is `reports/OPERATOR_RUNBOOK.md`.

## Operational limitations
- Queue and job store are in-memory and are not durable across process restarts.
- Delivery is simulated using local fake adapters; no real customer email is sent.
- Retry backoff values are recorded but are not time-scheduled.
- Timeout metadata does not forcibly interrupt synchronous calls.
- Metrics and idempotency state are in-memory demonstration components.

A production deployment would require durable storage, a persistent message broker, durable idempotency, real delivery integration, enforceable timeouts, scheduled retry/backoff, and production monitoring.

## Reflection (approximately 300 words)
Building this background-job workflow helped me understand why long-running operations should be separated from the request-response path. In the invoice scenario, the API should acknowledge a request quickly rather than making the customer wait while a PDF is generated and delivered. Returning a job ID immediately creates a clear boundary between accepting work and completing work. A separate worker can then process the queued job independently.

The state model was an important foundation. Defining the permitted transitions made the workflow easier to reason about and helped prevent invalid lifecycle changes. Events and traces added a record of what happened during execution, while the status service made it possible to inspect a job without directly changing it.

The reliability tasks showed that retries need more than simply running the same operation again. Transient failures may recover, while permanent failures should not be retried repeatedly. Idempotency and replay protection are important because duplicate triggers can otherwise create repeated side effects, such as sending the same invoice more than once. The fake delivery adapter made these behaviors testable without depending on an external email service.

The observability work connected the individual components. Metrics for submissions, completions, failures, retries, queue depth, delivery success, and latency provide useful operational signals. Redacting sensitive fields before exporting evidence is also an important habit when logs and reports may be shared.

Finally, the runbook and evidence validator made the implementation easier to operate and review. The completed assessment passed all 37 tests and validated all four required evidence artifacts. I also identified limitations that would need to be addressed before production use, especially in-memory storage, simulated delivery, non-scheduled backoff, and non-enforcing timeout metadata. Overall, this project strengthened my understanding of asynchronous execution, lifecycle contracts, reliability, observability, and operational documentation.
