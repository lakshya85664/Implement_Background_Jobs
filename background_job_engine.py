
"""Background invoice job queue, API service, and worker."""

import json
import uuid
from collections import deque
from pathlib import Path
from typing import Protocol

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from background_jobs import (
    BackgroundJob,
    InvoiceJobInput,
    JobResult,
    JobStatus,
    create_invoice_job,
    record_event,
    transition_job,
    utc_now,
)


ARTIFACTS_DIR = Path("artifacts")


class JobStore:
    """In-memory store for background job records."""

    def __init__(self) -> None:
        self.jobs: dict[str, BackgroundJob] = {}

    def save(self, job: BackgroundJob) -> None:
        self.jobs[job.job_id] = job

    def get(self, job_id: str) -> BackgroundJob | None:
        return self.jobs.get(job_id)


class JobQueue:
    """FIFO queue containing job IDs awaiting worker execution."""

    def __init__(self) -> None:
        self._items: deque[str] = deque()

    def enqueue(self, job_id: str) -> None:
        self._items.append(job_id)

    def dequeue(self) -> str | None:
        if not self._items:
            return None
        return self._items.popleft()

    @property
    def pending_count(self) -> int:
        return len(self._items)


class DeliveryAdapter(Protocol):
    """Boundary for delivering a generated invoice to a customer."""

    def deliver(self, email: str, pdf_path: str) -> str:
        ...


class FakeDeliveryAdapter:
    """Local fake delivery service used by this assessment."""

    def __init__(self) -> None:
        self.deliveries: list[dict[str, str]] = []

    def deliver(self, email: str, pdf_path: str) -> str:
        delivery_id = f"DEL-{len(self.deliveries) + 1:04d}"

        self.deliveries.append(
            {
                "delivery_id": delivery_id,
                "email": email,
                "pdf_path": pdf_path,
                "status": "delivered",
            }
        )

        return delivery_id


class InvoicePDFGenerator:
    """Generate a real invoice PDF using ReportLab."""

    def __init__(self, output_dir: Path = ARTIFACTS_DIR) -> None:
        self.output_dir = output_dir

    def generate(self, invoice: InvoiceJobInput) -> str:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        output_path = self.output_dir / f"{invoice.invoice_id}.pdf"

        pdf = canvas.Canvas(str(output_path), pagesize=A4)
        pdf.setTitle(f"Invoice {invoice.invoice_id}")

        pdf.setFont("Helvetica-Bold", 20)
        pdf.drawString(60, 790, "INVOICE")

        pdf.setFont("Helvetica", 12)
        pdf.drawString(60, 750, f"Invoice ID: {invoice.invoice_id}")
        pdf.drawString(60, 725, f"Customer ID: {invoice.customer_id}")
        pdf.drawString(60, 700, f"Customer Email: {invoice.customer_email}")
        pdf.drawString(
            60,
            660,
            f"Amount: {invoice.currency} {invoice.amount:,.2f}",
        )

        pdf.drawString(60, 610, "Thank you for your business.")
        pdf.save()

        return str(output_path)


class BackgroundJobAPI:
    """Accept a job request and return without running the worker."""

    def __init__(self, store: JobStore, queue: JobQueue) -> None:
        self.store = store
        self.queue = queue

    def submit(self, invoice: InvoiceJobInput) -> dict[str, str]:
        job_id = f"JOB-{uuid.uuid4().hex[:8].upper()}"

        job = create_invoice_job(
            job_id=job_id,
            business_key=f"invoice:{invoice.invoice_id}",
            job_input=invoice,
        )

        self.store.save(job)
        self.queue.enqueue(job_id)

        record_event(
            job,
            event_type="job.queued",
            details={"queue": "invoice_jobs"},
        )

        return {
            "job_id": job_id,
            "status": job.status.value,
            "message": "Invoice job accepted for background processing",
        }


class BackgroundJobWorker:
    """Process queued invoice jobs independently of API submission."""

    def __init__(
        self,
        store: JobStore,
        queue: JobQueue,
        pdf_generator: InvoicePDFGenerator,
        delivery_adapter: DeliveryAdapter,
    ) -> None:
        self.store = store
        self.queue = queue
        self.pdf_generator = pdf_generator
        self.delivery_adapter = delivery_adapter

    def process_next(self) -> dict[str, str] | None:
        job_id = self.queue.dequeue()

        if job_id is None:
            return None

        job = self.store.get(job_id)

        if job is None:
            return {
                "job_id": job_id,
                "status": "not_found",
            }

        if job.status != JobStatus.QUEUED:
            return {
                "job_id": job_id,
                "status": job.status.value,
            }

        transition_job(job, "start")

        pdf_path = self.pdf_generator.generate(job.input)

        delivery_id = self.delivery_adapter.deliver(
            job.input.customer_email,
            pdf_path,
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
            details={"delivery_id": delivery_id},
        )

        return {
            "job_id": job.job_id,
            "status": job.status.value,
            "pdf_path": pdf_path,
            "delivery_id": delivery_id,
        }


def run_task2_demo() -> dict:
    """Demonstrate immediate API acceptance and later worker processing."""

    store = JobStore()
    queue = JobQueue()
    delivery = FakeDeliveryAdapter()

    api = BackgroundJobAPI(store, queue)
    worker = BackgroundJobWorker(
        store=store,
        queue=queue,
        pdf_generator=InvoicePDFGenerator(),
        delivery_adapter=delivery,
    )

    invoice = InvoiceJobInput(
        invoice_id="INV-TASK2-001",
        customer_id="CUST-TASK2-001",
        customer_email="customer@example.com",
        amount=2750.00,
        currency="INR",
    )

    # API accepts the job and returns before worker execution.
    api_response = api.submit(invoice)
    job_after_submission = store.get(api_response["job_id"])

    # Capture the state before the worker starts.
    accepted_status = job_after_submission.status.value

    # The worker executes the job separately.
    worker_response = worker.process_next()
    final_job = store.get(api_response["job_id"])

    result = {
        "api_response": api_response,
        "status_immediately_after_submission": accepted_status,
        "queue_pending_after_submission": 1,
        "worker_response": worker_response,
        "final_status": final_job.status.value,
        "attempts": final_job.attempts,
        "result": final_job.result.model_dump(mode="json"),
        "delivery_count": len(delivery.deliveries),
        "queue_pending_after_processing": queue.pending_count,
    }

    Path("outputs").mkdir(exist_ok=True)
    Path("outputs/task2_background_job.json").write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    return result


if __name__ == "__main__":
    print(json.dumps(run_task2_demo(), indent=2))
