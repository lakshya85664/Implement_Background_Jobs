
import json

from background_job_runbook import (
    EXPECTED_EVIDENCE,
    EXPECTED_PDFS,
    generate_evidence_report,
    generate_operator_runbook,
    read_json_file,
    validate_pdf,
    validate_task2_evidence,
    validate_task4_evidence,
)


def valid_task2_data():
    return {
        "api_response": {
            "job_id": "JOB-001",
            "status": "queued",
        },
        "status_immediately_after_submission": "queued",
        "worker_response": {
            "status": "completed",
        },
        "final_status": "completed",
        "result": {
            "success": True,
            "pdf_path": "artifacts/invoice.pdf",
            "delivery_id": "DEL-001",
        },
        "queue_pending_after_processing": 0,
    }


def valid_task4_data():
    return {
        "accepted": {
            "job_id": "JOB-002",
            "status": "queued",
        },
        "status_before_processing": {
            "status": "queued",
        },
        "worker_result": {
            "status": "completed",
        },
        "status_after_processing": {
            "status": "completed",
        },
        "metrics": {
            "jobs_submitted": 1,
            "jobs_completed": 1,
            "delivery_success_rate": 1.0,
        },
        "evidence_job_count": 1,
    }


def test_task2_evidence_validation_accepts_valid_data():
    errors = validate_task2_evidence(valid_task2_data())

    assert errors == []


def test_task2_evidence_validation_detects_invalid_status():
    data = valid_task2_data()
    data["final_status"] = "failed"

    errors = validate_task2_evidence(data)

    assert "final_job_status_not_completed" in errors


def test_task4_evidence_validation_accepts_valid_data():
    errors = validate_task4_evidence(valid_task4_data())

    assert errors == []


def test_task4_evidence_validation_detects_missing_metrics():
    data = valid_task4_data()
    data["metrics"]["jobs_completed"] = 0

    errors = validate_task4_evidence(data)

    assert "completion_metric_missing" in errors


def test_read_json_file_detects_invalid_json(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text("{invalid json", encoding="utf-8")

    data, error = read_json_file(path)

    assert data is None
    assert error.startswith("invalid_json")


def test_pdf_validation_checks_pdf_signature(tmp_path):
    valid_path = tmp_path / "valid.pdf"
    invalid_path = tmp_path / "invalid.pdf"

    valid_path.write_bytes(b"%PDF-1.4\nsample")
    invalid_path.write_bytes(b"not a pdf")

    valid, message = validate_pdf(valid_path)
    invalid, invalid_message = validate_pdf(invalid_path)

    assert valid is True
    assert message == "valid_pdf"

    assert invalid is False
    assert invalid_message == "invalid_pdf_signature"


def test_runbook_is_generated(tmp_path):
    output_path = tmp_path / "reports" / "OPERATOR_RUNBOOK.md"

    generated_path = generate_operator_runbook(output_path)

    assert output_path.exists()
    assert generated_path == str(output_path)

    content = output_path.read_text(encoding="utf-8")

    assert "Failure recovery" in content
    assert "Duplicate and replay protection" in content
    assert "Operator checklist" in content


def test_evidence_report_checks_expected_files(tmp_path):
    for relative_path in EXPECTED_EVIDENCE.values():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)

        if relative_path.name == "task2_background_job.json":
            data = valid_task2_data()
        else:
            data = valid_task4_data()

        path.write_text(json.dumps(data), encoding="utf-8")

    for relative_path in EXPECTED_PDFS.values():
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\nsample")

    report = generate_evidence_report(tmp_path)

    assert report["expected_evidence_count"] == 4
    assert report["found_evidence_count"] == 4
    assert report["valid_evidence_count"] == 4
    assert report["evidence_complete"] is True

    report_path = tmp_path / "reports" / "evidence_report.json"
    operator_path = tmp_path / "outputs" / "task5_operator_report.json"

    assert report_path.exists()
    assert operator_path.exists()

    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert loaded["evidence_complete"] is True
