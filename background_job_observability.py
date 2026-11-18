
"""Integration boundaries, execution metrics, status lookup, and redaction."""

import json
import time
from pathlib import Path
from typing import Any, Protocol

from background_jobs import (
    BackgroundJob,
    JobStatus,
    JobTrace,
    utc_now,
)
from background_job_engine import (
    DeliveryAdapter,
    FakeDeliveryAdapter,
    InvoicePDFGenerator,
    JobQueue,
    JobStore,
)
from background_job_reliability import (
    IdempotencyLedger,
    ReliabilityPolicy,
    ReliableBackgroundJobWorker,
)


SENSITIVE_KEYS = {
    "password",
    "token",
    "access_token",
    "api_key",
    "secret",
    "authorization",
    "customer_email",
}


def redact_sensitive_data(value: Any) -> Any:
    """Recursively redact sensitive dictionary values."""

    if isinstance(value, dict):
        cleaned = {}

        for key, item in value.items():
            if key.lower() in SENSITIVE_KEYS:
                cleaned[key] = "[REDACTED]"
            else:
                cleaned[key] = redact_sensitive_data(item)

        return cleaned

    if isinstance(value, list):
        return [redact_sensitive_data(item) for item in value]

    if isinstance(value, tuple):
        return [redact_sensitive_data(item) for item in value]

    return value


class ObservableDeliveryAdapter(Protocol):
    """Contract for an observable delivery integration."""

    def deliver(self, email: str, pdf_path: str) -> str:
        ...


class InstrumentedFakeDeliveryAdapter:
    """Fake delivery adapter that records calls, latency, and outcomes."""

    def __init__(
        self,
        should_fail: bool = False,
        latency_seconds: float = 0.0,
    ) -> None:
        self.should_fail = should_fail
        self.latency_seconds = latency_seconds
        self.calls = 0
        self.successes = 0
        self.failures = 0
        self.total_latency_seconds = 0.0
        self.records: list[dict[str, Any]] = []

    def deliver(self, email: str, pdf_path: str) -> str:
        self.calls += 1
        started = time.perf_counter()

        try:
            if self.latency_seconds > 0:
                time.sleep(self.latency_seconds)

            if self.should_fail:
                raise ConnectionError("Simulated delivery failure")

            delivery_id = f"OBS-DEL-{self.successes + 1:04d}"
            self.successes += 1

            self.records.append(
                {
                    "delivery_id": delivery_id,
                    "email": email,
                    "pdf_path": pdf_path,
                    "status": "delivered",
                }
            )

            return delivery_id

        except Exception:
            self.failures += 1
            raise

        finally:
            elapsed = time.perf_counter() - started
            self.total_latency_seconds += elapsed

    def metrics(self) -> dict[str, Any]:
        average_latency = (
            self.total_latency_seconds / self.calls
            if self.calls
            else 0.0
        )

        success_rate = (
            self.successes / self.calls
            if self.calls
            else 0.0
        )

        return {
            "delivery_calls": self.calls,
            "delivery_successes": self.successes,
            "delivery_failures": self.failures,
            "delivery_success_rate": round(success_rate, 4),
            "average_latency_seconds": round(average_latency, 6),
        }


class JobStatusService:
    """Read-only job status lookup service."""

    def __init__(self, store: JobStore) -> None:
        self.store = store

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get(job_id)

        if job is None:
            return None

        return {
            "job_id": job.job_id,
            "status": job.status.value,
            "attempts": job.attempts,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "result": (
                job.result.model_dump(mode="json")
                if job.result is not None
                else None
            ),
        }


class JobMetrics:
    """Aggregate job lifecycle and queue metrics."""

    def __init__(self) -> None:
        self.submitted = 0
        self.completed = 0
        self.failed = 0
        self.retried = 0

    def record_submission(self) -> None:
        self.submitted += 1

    def record_job(self, job: BackgroundJob) -> None:
        if job.status == JobStatus.COMPLETED:
            self.completed += 1
        elif job.status == JobStatus.FAILED:
            self.failed += 1

        retry_events = sum(
            1
            for event in job.events
            if event.event_type == "job.retry_scheduled"
        )
        self.retried += retry_events

    def snapshot(
        self,
        queue: JobQueue,
        adapter_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "jobs_submitted": self.submitted,
            "jobs_completed": self.completed,
            "jobs_failed": self.failed,
            "jobs_retried": self.retried,
            "queue_pending": queue.pending_count,
            **adapter_metrics,
        }


class ObservableBackgroundJobSystem:
    """Compose job storage, queue, worker, status, and metrics."""

    def __init__(
        self,
        store: JobStore,
        queue: JobQueue,
        pdf_generator: InvoicePDFGenerator,
        delivery_adapter: DeliveryAdapter,
        policy: ReliabilityPolicy | None = None,
    ) -> None:
        self.store = store
        self.queue = queue
        self.delivery_adapter = delivery_adapter
        self.metrics = JobMetrics()

        self.worker = ReliableBackgroundJobWorker(
            store=store,
            queue=queue,
            pdf_generator=pdf_generator,
            delivery_adapter=delivery_adapter,
            ledger=IdempotencyLedger(),
            policy=policy or ReliabilityPolicy(),
        )

        self.status_service = JobStatusService(store)

    def submit(self, invoice) -> dict[str, str]:
        """Submit through the existing API and count the submission."""

        from background_job_engine import BackgroundJobAPI

        api = BackgroundJobAPI(self.store, self.queue)
        response = api.submit(invoice)

        self.metrics.record_submission()

        job = self.store.get(response["job_id"])
        job.traces.append(
            JobTrace(
                job_id=job.job_id,
                action="observability.submitted",
                status=job.status,
                attempt=job.attempts,
                details={"queue_pending": self.queue.pending_count},
            )
        )

        return response

    def process_next(self) -> dict[str, Any] | None:
        """Process one job and update aggregate metrics."""

        result = self.worker.process_next()

        if result is None:
            return None

        job = self.store.get(result["job_id"])

        if job is not None:
            job.traces.append(
                JobTrace(
                    job_id=job.job_id,
                    action="observability.worker_finished",
                    status=job.status,
                    attempt=job.attempts,
                    details={
                        "next_action": result.get("next_action"),
                    },
                )
            )

            self.metrics.record_job(job)

        return result

    def metrics_snapshot(self) -> dict[str, Any]:
        adapter_metrics = (
            self.delivery_adapter.metrics()
            if hasattr(self.delivery_adapter, "metrics")
            else {}
        )

        return self.metrics.snapshot(
            queue=self.queue,
            adapter_metrics=adapter_metrics,
        )

    
    def export_evidence(
        self,
        output_path: str = "outputs/task4_observability.json",
        lifecycle_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Export redacted metrics, job lifecycle evidence, and traces."""

        jobs = []

        for job in self.store.jobs.values():
            jobs.append(
                {
                    "job_id": job.job_id,
                    "status": job.status.value,
                    "attempts": job.attempts,
                    "events": [
                        event.model_dump(mode="json")
                        for event in job.events
                    ],
                    "traces": [
                        trace.model_dump(mode="json")
                        for trace in job.traces
                    ],
                }
            )

        lifecycle_evidence = lifecycle_evidence or {}

        evidence = {
            "generated_at": utc_now().isoformat(),

            # API and worker lifecycle evidence
            "accepted_response": lifecycle_evidence.get(
                "accepted_response"
            ),
            "status_before_processing": lifecycle_evidence.get(
                "status_before_processing"
            ),
            "worker_result": lifecycle_evidence.get(
                "worker_result"
            ),
            "status_after_processing": lifecycle_evidence.get(
                "status_after_processing"
            ),

            # Evidence count
            "evidence_job_count": len(jobs),

            # Existing observability information
            "metrics": self.metrics_snapshot(),
            "jobs": jobs,
        }

        evidence = redact_sensitive_data(evidence)

        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        path.write_text(
            json.dumps(evidence, indent=2),
            encoding="utf-8",
        )

        return evidence



def run_task4_demo() -> dict[str, Any]:
    """Run an observable successful background invoice job."""

    store = JobStore()
    queue = JobQueue()

    adapter = InstrumentedFakeDeliveryAdapter(
        should_fail=False,
        latency_seconds=0.001,
    )

    system = ObservableBackgroundJobSystem(
        store=store,
        queue=queue,
        pdf_generator=InvoicePDFGenerator(
            Path("artifacts/task4")
        ),
        delivery_adapter=adapter,
    )

    from background_jobs import InvoiceJobInput

    invoice = InvoiceJobInput(
        invoice_id="INV-TASK4-001",
        customer_id="CUST-TASK4-001",
        customer_email="task4@example.com",
        amount=4500.00,
        currency="INR",
    )

    accepted = system.submit(invoice)
    status_before = system.status_service.get_status(
        accepted["job_id"]
    )

    worker_result = system.process_next()
    status_after = system.status_service.get_status(
        accepted["job_id"]
    )

    evidence = system.export_evidence(
    lifecycle_evidence={
        "accepted_response": accepted,
        "status_before_processing": status_before,
        "worker_result": worker_result,
        "status_after_processing": status_after,
    }
)

    return {
        "accepted": accepted,
        "status_before_processing": status_before,
        "worker_result": worker_result,
        "status_after_processing": status_after,
        "metrics": system.metrics_snapshot(),
        "evidence_file": "outputs/task4_observability.json",
        "evidence_job_count": len(evidence["jobs"]),
    }


if __name__ == "__main__":
    print(json.dumps(run_task4_demo(), indent=2))
