
import pytest
from pydantic import ValidationError

from background_jobs import (
    BackgroundJob,
    InvoiceJobInput,
    JobStatus,
    create_invoice_job,
    transition_job,
)


@pytest.fixture
def sample_job() -> BackgroundJob:
    job_input = InvoiceJobInput(
        invoice_id="INV-TEST-01",
        customer_id="CUST-TEST-01",
        customer_email="test@example.com",
        amount=1500.00,
    )

    return create_invoice_job(
        job_id="JOB-TEST-01",
        business_key="invoice:INV-TEST-01",
        job_input=job_input,
    )


def test_new_job_starts_queued(sample_job):
    assert sample_job.status == JobStatus.QUEUED
    assert sample_job.attempts == 0
    assert len(sample_job.events) == 1


def test_queued_job_can_start(sample_job):
    transition_job(sample_job, "start")

    assert sample_job.status == JobStatus.RUNNING
    assert sample_job.attempts == 1
    assert sample_job.events[-1].event_type == "start"


def test_running_job_can_complete(sample_job):
    transition_job(sample_job, "start")
    transition_job(sample_job, "complete")

    assert sample_job.status == JobStatus.COMPLETED
    assert len(sample_job.traces) == 3


def test_running_job_can_fail(sample_job):
    transition_job(sample_job, "start")
    transition_job(sample_job, "fail")

    assert sample_job.status == JobStatus.FAILED


def test_failed_job_can_be_requeued(sample_job):
    transition_job(sample_job, "start")
    transition_job(sample_job, "fail")
    transition_job(sample_job, "retry")

    assert sample_job.status == JobStatus.QUEUED


def test_illegal_transition_is_rejected(sample_job):
    with pytest.raises(ValueError, match="Illegal job transition"):
        transition_job(sample_job, "complete")

    assert sample_job.status == JobStatus.QUEUED


def test_completed_job_cannot_be_started_again(sample_job):
    transition_job(sample_job, "start")
    transition_job(sample_job, "complete")

    with pytest.raises(ValueError, match="Illegal job transition"):
        transition_job(sample_job, "start")

    assert sample_job.status == JobStatus.COMPLETED


def test_invalid_invoice_amount_is_rejected():
    with pytest.raises(ValidationError):
        InvoiceJobInput(
            invoice_id="INV-INVALID",
            customer_id="CUST-INVALID",
            customer_email="test@example.com",
            amount=-100,
        )
