# Background Invoice Jobs — Operator Runbook

## 1. System overview

The system accepts invoice-generation requests through an API service.
The API creates a background job and places its ID into a queue. A worker
later generates the invoice PDF and invokes the delivery adapter.

The implementation uses an in-memory job store and FIFO queue for this
assessment. The delivery integration is simulated with local fake adapters.

## 2. Job lifecycle

| State | Meaning | Operator action |
|---|---|---|
| queued | Accepted and waiting for a worker | Check queue and worker availability |
| running | Worker is processing the invoice | Check execution traces and dependency health |
| completed | PDF generation and delivery succeeded | Verify result and delivery ID |
| failed | Processing failed | Inspect error type and determine recovery |

Permitted transitions:

- queued -> running
- running -> completed
- running -> failed
- failed -> queued

## 3. Normal processing procedure

1. Submit an invoice request.
2. Record the returned job ID.
3. Confirm that the initial status is queued.
4. Allow a worker to claim the job.
5. Verify that the PDF artifact exists.
6. Verify that the delivery adapter returns a delivery ID.
7. Confirm that the job reaches completed.
8. Check execution events, traces, and metrics.

## 4. Failure recovery

### Transient failures

Examples include timeouts and connection errors.

1. Inspect the recorded error and attempt number.
2. Check whether the configured retry limit has been reached.
3. If another attempt is allowed, the job returns to queued.
4. Confirm that the next worker attempt succeeds.
5. If retries are exhausted, escalate for operator review.

### Permanent failures

Examples include invalid responses and rejected delivery requests.

1. Inspect the error classification.
2. Do not repeatedly retry a permanent failure.
3. Confirm that the job remains failed.
4. Correct the underlying business input or integration issue.
5. Follow an approved recovery procedure before resubmission.

## 5. Duplicate and replay protection

- Use the job ID as the execution identity.
- Reject duplicate queue triggers for ineligible jobs.
- Reuse a recorded delivery result for a completed side effect.
- Return the cached result when a completed job is replayed.
- Never assume that a repeated API request represents a new invoice.

## 6. Observability and data protection

Inspect:

- Job status and attempt count.
- Lifecycle events and execution traces.
- Queue depth.
- Submission, completion, failure, and retry metrics.
- Delivery calls, failures, success rate, and latency.

Sensitive fields must be redacted before exporting evidence.

## 7. Evidence files

- outputs/task2_background_job.json
- outputs/task4_observability.json
- outputs/task5_operator_report.json
- reports/evidence_report.json
- reports/OPERATOR_RUNBOOK.md
- artifacts/INV-TASK2-001.pdf
- artifacts/task4/INV-TASK4-001.pdf

## 8. Known limitations

- Job storage and queue are in-memory, not durable.
- The local delivery adapter does not send real email.
- Retry backoff is recorded but not time-scheduled.
- Timeout policy metadata does not forcibly interrupt synchronous calls.
- Metrics are in-memory demonstration metrics.
- Production deployment would require durable storage, a persistent
  message broker, real delivery integration, and operational monitoring.

## 9. Operator checklist

- [ ] Confirm the job ID.
- [ ] Confirm the initial queued state.
- [ ] Check worker execution.
- [ ] Verify the generated PDF.
- [ ] Verify the delivery ID.
- [ ] Confirm the final job status.
- [ ] Inspect errors, events, and traces.
- [ ] Validate the evidence report.
- [ ] Escalate permanent failures or exhausted retries.
