
# Background Jobs — State Transition Model

## Job lifecycle

A background invoice job follows four lifecycle states:

- **queued** — Job has been accepted and is waiting for a worker.
- **running** — A worker has claimed the job and is processing it.
- **completed** — Invoice generation and customer delivery have succeeded.
- **failed** — Processing has failed and requires recovery or operator action.

## Transition table

| Current state | Event | Next state | Description |
|---|---|---|---|
| queued | start | running | Worker begins processing |
| running | complete | completed | Job finishes successfully |
| running | fail | failed | Job encounters a terminal failure |
| failed | retry | queued | Failed job is made eligible for retry |

All other transitions are rejected.

## Lifecycle diagram

```text
                 start
    QUEUED -----------------> RUNNING
                                |
                     +----------+----------+
                     |                     |
                  complete                fail
                     |                     |
                     v                     v
                 COMPLETED              FAILED
                                           |
                                         retry
                                           |
                                           v
                                         QUEUED
```

## Model responsibilities

- InvoiceJobInput validates invoice and customer information.
- BackgroundJob stores job identity, business key, state, attempts, and history.
- JobEvent records state changes and event details.
- JobResult stores the final output or error information.
- JobTrace records structured execution information.

## State safety

The transition function checks every state/event combination before changing
the job state. An illegal transition raises ValueError. This prevents invalid
lifecycle changes from silently modifying a job.
