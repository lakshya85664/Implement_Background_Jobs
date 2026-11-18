
from pathlib import Path

import json

from background_jobs import InvoiceJobInput, JobStatus
from background_job_engine import (
    InvoicePDFGenerator,
    JobQueue,
    JobStore,
)
from background_job_observability import (
    InstrumentedFakeDeliveryAdapter,
    JobStatusService,
    ObservableBackgroundJobSystem,
    redact_sensitive_data,
)


def create_system(tmp_path, should_fail=False):
    store = JobStore()
    queue = JobQueue()

    adapter = InstrumentedFakeDeliveryAdapter(
        should_fail=should_fail,
        latency_seconds=0,
    )

    system = ObservableBackgroundJobSystem(
        store=store,
        queue=queue,
        pdf_generator=InvoicePDFGenerator(
            tmp_path / "pdfs"
        ),
        delivery_adapter=adapter,
    )

    return store, queue, adapter, system


def create_invoice(invoice_id="INV-OBS-001"):
    return InvoiceJobInput(
        invoice_id=invoice_id,
        customer_id="CUST-OBS-001",
        customer_email="observer@example.com",
        amount=2400,
        currency="INR",
    )


def test_adapter_records_success_metrics(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    system.submit(create_invoice())
    result = system.process_next()

    metrics = adapter.metrics()

    assert result["status"] == "completed"
    assert metrics["delivery_calls"] == 1
    assert metrics["delivery_successes"] == 1
    assert metrics["delivery_failures"] == 0
    assert metrics["delivery_success_rate"] == 1.0


def test_adapter_records_failure_metrics(tmp_path):
    store, queue, adapter, system = create_system(
        tmp_path,
        should_fail=True,
    )

    system.submit(create_invoice())
    result = system.process_next()

    metrics = adapter.metrics()

    assert result["status"] == "queued"
    assert metrics["delivery_calls"] == 1
    assert metrics["delivery_failures"] == 1
    assert metrics["delivery_successes"] == 0


def test_status_lookup_returns_current_job_state(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    accepted = system.submit(create_invoice())

    status_service = JobStatusService(store)
    status = status_service.get_status(accepted["job_id"])

    assert status is not None
    assert status["status"] == JobStatus.QUEUED.value

    system.process_next()

    final_status = status_service.get_status(accepted["job_id"])

    assert final_status["status"] == JobStatus.COMPLETED.value


def test_unknown_job_status_returns_none(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    assert system.status_service.get_status("UNKNOWN-JOB") is None


def test_metrics_track_submission_and_completion(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    system.submit(create_invoice())
    system.process_next()

    metrics = system.metrics_snapshot()

    assert metrics["jobs_submitted"] == 1
    assert metrics["jobs_completed"] == 1
    assert metrics["jobs_failed"] == 0
    assert metrics["queue_pending"] == 0


def test_recursive_redaction_removes_sensitive_values():
    payload = {
        "job_id": "JOB-1",
        "api_key": "secret-value",
        "nested": {
            "customer_email": "private@example.com",
            "safe_value": "visible",
            "items": [
                {"token": "hidden-token"},
                {"name": "invoice"},
            ],
        },
    }

    cleaned = redact_sensitive_data(payload)

    assert cleaned["api_key"] == "[REDACTED]"
    assert cleaned["nested"]["customer_email"] == "[REDACTED]"
    assert cleaned["nested"]["items"][0]["token"] == "[REDACTED]"
    assert cleaned["nested"]["safe_value"] == "visible"


def test_evidence_export_creates_redacted_json(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    system.submit(create_invoice())
    system.process_next()

    output_file = tmp_path / "evidence" / "task4.json"

    evidence = system.export_evidence(str(output_file))

    assert output_file.exists()

    loaded = json.loads(output_file.read_text(encoding="utf-8"))

    assert loaded["metrics"]["jobs_submitted"] == 1
    assert loaded["metrics"]["jobs_completed"] == 1
    assert len(loaded["jobs"]) == 1
    assert evidence["jobs"][0]["status"] == "completed"

    serialized = json.dumps(loaded)
    assert "observer@example.com" not in serialized


def test_execution_traces_are_recorded(tmp_path):
    store, queue, adapter, system = create_system(tmp_path)

    accepted = system.submit(create_invoice())
    system.process_next()

    job = store.get(accepted["job_id"])

    actions = [trace.action for trace in job.traces]

    assert "observability.submitted" in actions
    assert "job_completed" in actions
    assert "observability.worker_finished" in actions
