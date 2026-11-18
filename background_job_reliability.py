
"""Failure handling, bounded retries, and replay safety for background jobs."""

from dataclasses import dataclass, field
from typing import Any

from background_jobs import (
    JobResult,
    JobStatus,
    JobTrace,
    record_event,
    transition_job,
    utc_now,
)

from background_job_engine import (
    DeliveryAdapter,
    InvoicePDFGenerator,
    JobQueue,
    JobStore,
)


@dataclass
class IdempotencyLedger:
    """Remember successful side effects to prevent duplicate delivery."""

    deliveries: dict[str, str] = field(default_factory=dict)

    def get_delivery(self, idempotency_key: str) -> str | None:
        return self.deliveries.get(idempotency_key)

    def record_delivery(
        self,
        idempotency_key: str,
        delivery_id: str,
    ) -> None:
        self.deliveries[idempotency_key] = delivery_id


@dataclass
class ReliabilityPolicy:
    """Bounded retry and timeout policy for job execution."""

    max_attempts: int = 3
    timeout_seconds: float = 5.0
    backoff_seconds: tuple[int, ...] = (1, 2)

    def backoff_for(self, failed_attempt: int) -> int:
        index = failed_attempt - 1

        if index < len(self.backoff_seconds):
            return self.backoff_seconds[index]

        return (
            self.backoff_seconds[-1]
            if self.backoff_seconds
            else 0
        )


class FailureClassifier:
    """Classify worker errors into transient and permanent failures."""

    TRANSIENT_ERRORS = (TimeoutError, ConnectionError)

    @classmethod
    def classify(cls, error: Exception) -> str:
        if isinstance(error, cls.TRANSIENT_ERRORS):
            return "transient"

        return "permanent"


class ReliableBackgroundJobWorker:
    """Execute jobs with retries, failure classification, and replay safety."""

    def __init__(
        self,
        store: JobStore,
        queue: JobQueue,
        pdf_generator: InvoicePDFGenerator,
        delivery_adapter: DeliveryAdapter,
        ledger: IdempotencyLedger | None = None,
        policy: ReliabilityPolicy | None = None,
    ) -> None:
        self.store = store
        self.queue = queue
        self.pdf_generator = pdf_generator
        self.delivery_adapter = delivery_adapter
        self.ledger = ledger or IdempotencyLedger()
        self.policy = policy or ReliabilityPolicy()
        self._processing: set[str] = set()

    def enqueue_once(self, job_id: str) -> bool:
        """Queue a job only if eligible and not already queued."""

        job = self.store.get(job_id)

        if job is None:
            return False

        if job.status != JobStatus.QUEUED:
            return False

        if job_id in self._processing:
            return False

        # Prevent duplicate queue entries.
        if job_id in self.queue._items:
            return False

        self.queue.enqueue(job_id)
        return True

    def _deliver_once(
        self,
        job_id: str,
        email: str,
        pdf_path: str,
    ) -> str:
        """Deliver once per job ID and reuse the stored delivery result."""

        existing_delivery = self.ledger.get_delivery(job_id)

        if existing_delivery is not None:
            return existing_delivery

        delivery_id = self.delivery_adapter.deliver(
            email,
            pdf_path,
        )

        if not isinstance(delivery_id, str) or not delivery_id.strip():
            raise ValueError(
                "Malformed delivery response: missing delivery ID"
            )

        self.ledger.record_delivery(job_id, delivery_id)

        return delivery_id

    def _handle_failure(
        self,
        job: Any,
        error: Exception,
        error_type: str,
    ) -> dict[str, Any]:
        """Record failure and determine the next recovery action."""

        if job.status == JobStatus.RUNNING:
            transition_job(job, "fail")

        job.result = JobResult(
            success=False,
            invoice_id=job.input.invoice_id,
            error_type=error_type,
            error_message=str(error),
            completed_at=utc_now(),
        )

        retryable = error_type == "transient"

        can_retry = (
            retryable
            and job.attempts < self.policy.max_attempts
        )

        if can_retry:
            delay = self.policy.backoff_for(job.attempts)

            transition_job(job, "retry")
            self.queue.enqueue(job.job_id)

            next_action = "retry"

            record_event(
                job,
                event_type="job.retry_scheduled",
                details={
                    "failed_attempt": job.attempts,
                    "next_attempt": job.attempts + 1,
                    "backoff_seconds": delay,
                    "error_type": error_type,
                },
            )

        else:
            delay = 0
            next_action = "operator_review"

            record_event(
                job,
                event_type="job.exhausted_or_permanent",
                details={
                    "attempts": job.attempts,
                    "max_attempts": self.policy.max_attempts,
                    "error_type": error_type,
                },
            )

        job.traces.append(
            JobTrace(
                job_id=job.job_id,
                action="failure_handled",
                status=job.status,
                attempt=job.attempts,
                details={
                    "error_type": error_type,
                    "next_action": next_action,
                    "backoff_seconds": delay,
                },
            )
        )

        return {
            "job_id": job.job_id,
            "status": job.status.value,
            "error_type": error_type,
            "error": str(error),
            "attempts": job.attempts,
            "next_action": next_action,
            "backoff_seconds": delay,
        }

    def process_next(self) -> dict[str, Any] | None:
        """Process one queued job or return None for an empty queue."""

        job_id = self.queue.dequeue()

        if job_id is None:
            return None

        job = self.store.get(job_id)

        if job is None:
            return {
                "job_id": job_id,
                "status": "not_found",
                "next_action": "operator_review",
            }

        # Completed, failed, or otherwise ineligible jobs are not executed.
        if job.status != JobStatus.QUEUED:
            return {
                "job_id": job_id,
                "status": job.status.value,
                "next_action": "no_op",
            }

        if job_id in self._processing:
            return {
                "job_id": job_id,
                "status": job.status.value,
                "next_action": "already_processing",
            }

        self._processing.add(job_id)

        try:
            transition_job(job, "start")

            record_event(
                job,
                event_type="worker.started",
                details={
                    "attempt": job.attempts,
                    "timeout_seconds": self.policy.timeout_seconds,
                },
            )

            pdf_path = self.pdf_generator.generate(job.input)

            delivery_id = self._deliver_once(
                job_id=job.job_id,
                email=job.input.customer_email,
                pdf_path=pdf_path,
            )

            job.result = JobResult(
                success=True,
                invoice_id=job.input.invoice_id,
                pdf_path=pdf_path,
                delivery_id=delivery_id,
                completed_at=utc_now(),
            )

            transition_job(job, "complete")

            record_event(
                job,
                event_type="invoice.delivered",
                details={
                    "delivery_id": delivery_id,
                    "idempotency_key": job_id,
                },
            )

            job.traces.append(
                JobTrace(
                    job_id=job.job_id,
                    action="job_completed",
                    status=job.status,
                    attempt=job.attempts,
                    details={
                        "next_action": "none",
                    },
                )
            )

            return {
                "job_id": job.job_id,
                "status": job.status.value,
                "attempts": job.attempts,
                "pdf_path": pdf_path,
                "delivery_id": delivery_id,
                "next_action": "none",
            }

        except Exception as error:
            error_type = FailureClassifier.classify(error)

            return self._handle_failure(
                job=job,
                error=error,
                error_type=error_type,
            )

        finally:
            self._processing.discard(job_id)

    def replay(self, job_id: str) -> dict[str, Any]:
        """Return cached results without repeating completed side effects."""

        job = self.store.get(job_id)

        if job is None:
            return {
                "job_id": job_id,
                "status": "not_found",
                "next_action": "operator_review",
            }

        if job.status == JobStatus.COMPLETED:
            record_event(
                job,
                event_type="job.replay_ignored",
                details={
                    "reason": "job_already_completed",
                },
            )

            return {
                "job_id": job_id,
                "status": job.status.value,
                "result": job.result.model_dump(mode="json"),
                "next_action": "return_cached_result",
            }

        if job.status == JobStatus.QUEUED:
            self.enqueue_once(job_id)

            return {
                "job_id": job_id,
                "status": job.status.value,
                "next_action": "queued",
            }

        return {
            "job_id": job_id,
            "status": job.status.value,
            "next_action": "operator_review",
        }
