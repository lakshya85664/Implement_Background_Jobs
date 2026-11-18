
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class InvoiceJobInput(BaseModel):
    """Validated business input required to generate and deliver an invoice."""

    model_config = ConfigDict(str_strip_whitespace=True)

    invoice_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    customer_email: str = Field(min_length=5)
    amount: float = Field(gt=0)
    currency: str = Field(default="INR", min_length=3, max_length=3)


class JobEvent(BaseModel):
    """An event recording a meaningful job lifecycle change."""

    event_id: str
    job_id: str
    event_type: str
    previous_status: JobStatus | None = None
    current_status: JobStatus
    timestamp: datetime = Field(default_factory=utc_now)
    details: dict[str, Any] = Field(default_factory=dict)


class JobResult(BaseModel):
    """The final output or failure information for a background job."""

    success: bool
    invoice_id: str
    pdf_path: str | None = None
    delivery_id: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    completed_at: datetime | None = None


class JobTrace(BaseModel):
    """Structured trace information for one job execution."""

    job_id: str
    action: str
    status: JobStatus
    timestamp: datetime = Field(default_factory=utc_now)
    attempt: int = 0
    details: dict[str, Any] = Field(default_factory=dict)


class BackgroundJob(BaseModel):
    """Persistent workflow record representing one background job."""

    job_id: str
    business_key: str
    input: InvoiceJobInput
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    max_attempts: int = Field(default=3, ge=1)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    result: JobResult | None = None
    events: list[JobEvent] = Field(default_factory=list)
    traces: list[JobTrace] = Field(default_factory=list)


# Only these lifecycle transitions are permitted.
LEGAL_TRANSITIONS: dict[tuple[JobStatus, str], JobStatus] = {
    (JobStatus.QUEUED, "start"): JobStatus.RUNNING,
    (JobStatus.RUNNING, "complete"): JobStatus.COMPLETED,
    (JobStatus.RUNNING, "fail"): JobStatus.FAILED,
    (JobStatus.FAILED, "retry"): JobStatus.QUEUED,
}


def record_event(
    job: BackgroundJob,
    event_type: str,
    previous_status: JobStatus | None = None,
    details: dict[str, Any] | None = None,
) -> JobEvent:
    """Append a structured event to the job history."""

    event = JobEvent(
        event_id=f"EVT-{len(job.events) + 1:04d}",
        job_id=job.job_id,
        event_type=event_type,
        previous_status=previous_status,
        current_status=job.status,
        details=details or {},
    )
    job.events.append(event)
    return event


def transition_job(job: BackgroundJob, event_type: str) -> BackgroundJob:
    """Apply a legal lifecycle transition or reject an invalid one."""

    transition = (job.status, event_type)
    next_status = LEGAL_TRANSITIONS.get(transition)

    if next_status is None:
        raise ValueError(
            f"Illegal job transition: {job.status.value} + {event_type}"
        )

    previous_status = job.status
    job.status = next_status
    job.updated_at = utc_now()

    if event_type == "start":
        job.attempts += 1

    record_event(
        job,
        event_type=event_type,
        previous_status=previous_status,
        details={"attempt": job.attempts},
    )

    job.traces.append(
        JobTrace(
            job_id=job.job_id,
            action=event_type,
            status=job.status,
            attempt=job.attempts,
        )
    )

    return job


def create_invoice_job(
    job_id: str,
    business_key: str,
    job_input: InvoiceJobInput,
) -> BackgroundJob:
    """Create a new job in the queued state."""

    job = BackgroundJob(
        job_id=job_id,
        business_key=business_key,
        input=job_input,
    )

    record_event(
        job,
        event_type="job.created",
        details={"business_key": business_key},
    )

    job.traces.append(
        JobTrace(
            job_id=job.job_id,
            action="job.created",
            status=job.status,
        )
    )

    return job


def run_topic_fixture() -> dict[str, Any]:
    """Demonstrate creation and legal state transitions."""

    job_input = InvoiceJobInput(
        invoice_id="INV-1001",
        customer_id="CUST-101",
        customer_email="customer@example.com",
        amount=2500.00,
        currency="INR",
    )

    job = create_invoice_job(
        job_id="JOB-0001",
        business_key="invoice:INV-1001",
        job_input=job_input,
    )

    transition_job(job, "start")
    transition_job(job, "complete")

    job.result = JobResult(
        success=True,
        invoice_id=job.input.invoice_id,
        pdf_path="artifacts/INV-1001.pdf",
        delivery_id="DEL-1001",
        completed_at=utc_now(),
    )

    return {
        "job_id": job.job_id,
        "business_key": job.business_key,
        "status": job.status.value,
        "attempts": job.attempts,
        "result": job.result.model_dump(mode="json"),
        "events": [
            event.model_dump(mode="json") for event in job.events
        ],
        "traces": [
            trace.model_dump(mode="json") for trace in job.traces
        ],
    }


if __name__ == "__main__":
    import json

    print(json.dumps(run_topic_fixture(), indent=2))
