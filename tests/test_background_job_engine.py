
import pytest
from pydantic import ValidationError

from background_jobs import InvoiceJobInput, JobStatus
from background_job_engine import (
    BackgroundJobAPI,
    BackgroundJobWorker,
    FakeDeliveryAdapter,
    InvoicePDFGenerator,
    JobQueue,
    JobStore,
)


@pytest.fixture
def setup_system(tmp_path):
    store = JobStore()
    queue = JobQueue()
    delivery = FakeDeliveryAdapter()
    generator = InvoicePDFGenerator(output_dir=tmp_path / "pdfs")

    api = BackgroundJobAPI(store, queue)
    worker = BackgroundJobWorker(
        store=store,
        queue=queue,
        pdf_generator=generator,
        delivery_adapter=delivery,
    )

    return store, queue, delivery, api, worker


@pytest.fixture
def valid_invoice():
    return InvoiceJobInput(
        invoice_id="INV-TEST-2001",
        customer_id="CUST-2001",
        customer_email="customer@example.com",
        amount=1800.00,
        currency="INR",
    )


def test_api_returns_job_id_before_worker_runs(setup_system, valid_invoice):
    store, queue, delivery, api, worker = setup_system

    response = api.submit(valid_invoice)

    assert response["job_id"].startswith("JOB-")
    assert response["status"] == "queued"

    job = store.get(response["job_id"])
    assert job is not None
    assert job.status == JobStatus.QUEUED
    assert queue.pending_count == 1

    # API submission must not generate or deliver the invoice.
    assert delivery.deliveries == []


def test_worker_generates_pdf_and_delivers_invoice(
    setup_system, valid_invoice
):
    store, queue, delivery, api, worker = setup_system

    response = api.submit(valid_invoice)
    job_id = response["job_id"]

    worker_response = worker.process_next()
    job = store.get(job_id)

    assert worker_response["status"] == "completed"
    assert job.status == JobStatus.COMPLETED
    assert job.attempts == 1

    assert job.result is not None
    assert job.result.success is True
    assert job.result.invoice_id == valid_invoice.invoice_id

    pdf_path = job.result.pdf_path
    assert pdf_path is not None

    from pathlib import Path
    assert Path(pdf_path).exists()
    assert Path(pdf_path).read_bytes().startswith(b"%PDF")

    assert len(delivery.deliveries) == 1
    assert delivery.deliveries[0]["email"] == valid_invoice.customer_email
    assert queue.pending_count == 0


def test_empty_queue_returns_none(setup_system):
    _, _, _, _, worker = setup_system

    assert worker.process_next() is None


def test_invalid_invoice_is_rejected_before_queueing(setup_system):
    store, queue, _, api, _ = setup_system

    with pytest.raises(ValidationError):
        invalid_invoice = InvoiceJobInput(
            invoice_id="INV-INVALID",
            customer_id="CUST-INVALID",
            customer_email="customer@example.com",
            amount=-500,
        )
        api.submit(invalid_invoice)

    assert len(store.jobs) == 0
    assert queue.pending_count == 0


def test_worker_processes_only_one_job_per_call(setup_system):
    store, queue, delivery, api, worker = setup_system

    invoice_one = InvoiceJobInput(
        invoice_id="INV-ONE",
        customer_id="CUST-ONE",
        customer_email="one@example.com",
        amount=1000,
    )
    invoice_two = InvoiceJobInput(
        invoice_id="INV-TWO",
        customer_id="CUST-TWO",
        customer_email="two@example.com",
        amount=2000,
    )

    first = api.submit(invoice_one)
    second = api.submit(invoice_two)

    assert queue.pending_count == 2

    worker.process_next()

    assert store.get(first["job_id"]).status == JobStatus.COMPLETED
    assert store.get(second["job_id"]).status == JobStatus.QUEUED
    assert queue.pending_count == 1
    assert len(delivery.deliveries) == 1
