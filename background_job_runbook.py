
"""Operator runbook and evidence validation for background invoice jobs."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent

EXPECTED_EVIDENCE = {
    "task2_background_job": Path("outputs/task2_background_job.json"),
    "task4_observability": Path("outputs/task4_observability.json"),
}

EXPECTED_PDFS = {
    "task2_invoice_pdf": Path("artifacts/INV-TASK2-001.pdf"),
    "task4_invoice_pdf": Path("artifacts/task4/INV-TASK4-001.pdf"),
}


def utc_timestamp() -> str:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc).isoformat()


def read_json_file(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    """Read and validate a JSON object."""

    if not path.exists():
        return None, "file_missing"

    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        return None, f"invalid_json: {error}"

    if not isinstance(content, dict):
        return None, "json_root_must_be_an_object"

    return content, None


def validate_task2_evidence(data: dict[str, Any]) -> list[str]:
    """Validate required fields in Task 2 execution evidence."""

    errors = []

    api_response = data.get("api_response")
    if not isinstance(api_response, dict):
        errors.append("api_response_missing")
    else:
        if not api_response.get("job_id"):
            errors.append("api_job_id_missing")

        if api_response.get("status") != "queued":
            errors.append("api_initial_status_must_be_queued")

    if data.get("status_immediately_after_submission") != "queued":
        errors.append("job_was_not_queued_before_worker_execution")

    worker_response = data.get("worker_response")
    if not isinstance(worker_response, dict):
        errors.append("worker_response_missing")
    elif worker_response.get("status") != "completed":
        errors.append("worker_did_not_complete_job")

    result = data.get("result")
    if not isinstance(result, dict):
        errors.append("job_result_missing")
    else:
        if result.get("success") is not True:
            errors.append("job_result_not_successful")

        if not result.get("pdf_path"):
            errors.append("generated_pdf_path_missing")

        if not result.get("delivery_id"):
            errors.append("delivery_id_missing")

    if data.get("final_status") != "completed":
        errors.append("final_job_status_not_completed")

    if data.get("queue_pending_after_processing") != 0:
        errors.append("queue_not_empty_after_processing")

    return errors


def validate_task4_evidence(data: dict[str, Any]) -> list[str]:
    """Validate required fields in Task 4 observability evidence."""

    errors = []

    accepted = data.get("accepted", data.get("accepted_response"))
    if not isinstance(accepted, dict):
        errors.append("accepted_response_missing")
    elif accepted.get("status") != "queued":
        errors.append("accepted_job_status_not_queued")

    before = data.get("status_before_processing")
    if not isinstance(before, dict):
        errors.append("status_before_processing_missing")
    elif before.get("status") != "queued":
        errors.append("initial_status_not_queued")

    worker_result = data.get("worker_result")
    if not isinstance(worker_result, dict):
        errors.append("worker_result_missing")
    elif worker_result.get("status") != "completed":
        errors.append("worker_result_not_completed")

    after = data.get("status_after_processing")
    if not isinstance(after, dict):
        errors.append("status_after_processing_missing")
    elif after.get("status") != "completed":
        errors.append("final_status_not_completed")

    metrics = data.get("metrics")
    if not isinstance(metrics, dict):
        errors.append("metrics_missing")
    else:
        if metrics.get("jobs_submitted", 0) < 1:
            errors.append("submission_metric_missing")

        if metrics.get("jobs_completed", 0) < 1:
            errors.append("completion_metric_missing")

        if metrics.get("delivery_success_rate", 0) <= 0:
            errors.append("delivery_success_rate_invalid")

    if data.get("evidence_job_count", 0) < 1:
        errors.append("evidence_job_count_invalid")

    return errors


def validate_pdf(path: Path) -> tuple[bool, str]:
    """Check that a generated PDF exists and has a PDF signature."""

    if not path.exists():
        return False, "file_missing"

    try:
        with path.open("rb") as pdf_file:
            signature = pdf_file.read(5)
    except OSError as error:
        return False, f"read_error: {error}"

    if signature != b"%PDF-":
        return False, "invalid_pdf_signature"

    return True, "valid_pdf"


def generate_operator_runbook(output_path: Path) -> str:
    """Generate the operational guide for the background job system."""

    content = """# Background Invoice Jobs — Operator Runbook

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
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")

    return str(output_path)


def generate_evidence_report(
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Validate available evidence and generate a structured report."""

    evidence_results = {}
    pdf_results = {}

    for name, relative_path in EXPECTED_EVIDENCE.items():
        path = project_root / relative_path
        data, read_error = read_json_file(path)

        if read_error:
            evidence_results[name] = {
                "path": str(relative_path),
                "exists": path.exists(),
                "valid": False,
                "errors": [read_error],
            }
            continue

        if name == "task2_background_job":
            errors = validate_task2_evidence(data)
        elif name == "task4_observability":
            errors = validate_task4_evidence(data)
        else:
            errors = ["no_validator_defined"]

        evidence_results[name] = {
            "path": str(relative_path),
            "exists": True,
            "valid": len(errors) == 0,
            "errors": errors,
        }

    for name, relative_path in EXPECTED_PDFS.items():
        path = project_root / relative_path
        valid, message = validate_pdf(path)

        pdf_results[name] = {
            "path": str(relative_path),
            "exists": path.exists(),
            "valid": valid,
            "message": message,
        }

    expected_count = len(EXPECTED_EVIDENCE) + len(EXPECTED_PDFS)

    all_results = list(evidence_results.values()) + list(pdf_results.values())

    found_count = sum(1 for item in all_results if item["exists"])
    valid_count = sum(1 for item in all_results if item["valid"])

    report = {
        "generated_at": utc_timestamp(),
        "project": "Implement Background Jobs",
        "expected_evidence_count": expected_count,
        "found_evidence_count": found_count,
        "valid_evidence_count": valid_count,
        "evidence_complete": (
            found_count == expected_count
            and valid_count == expected_count
        ),
        "json_evidence": evidence_results,
        "pdf_artifacts": pdf_results,
        "limitations": [
            "The queue and job store are in-memory.",
            "Delivery is simulated using local fake adapters.",
            "Retry backoff is recorded but not time-scheduled.",
            "Timeout metadata does not interrupt synchronous calls.",
            "Metrics are in-memory demonstration metrics.",
        ],
    }

    report_path = project_root / "reports" / "evidence_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    operator_report_path = (
        project_root / "outputs" / "task5_operator_report.json"
    )
    operator_report_path.parent.mkdir(parents=True, exist_ok=True)
    operator_report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    return report


def run_task5() -> dict[str, Any]:
    """Generate the runbook and validate project evidence."""

    runbook_path = generate_operator_runbook(
        PROJECT_ROOT / "reports" / "OPERATOR_RUNBOOK.md"
    )

    report = generate_evidence_report(PROJECT_ROOT)

    return {
        "runbook": runbook_path,
        "evidence_report": (
            "reports/evidence_report.json"
        ),
        "operator_report": (
            "outputs/task5_operator_report.json"
        ),
        "expected_evidence_count": report["expected_evidence_count"],
        "found_evidence_count": report["found_evidence_count"],
        "valid_evidence_count": report["valid_evidence_count"],
        "evidence_complete": report["evidence_complete"],
    }


if __name__ == "__main__":
    print(json.dumps(run_task5(), indent=2))
