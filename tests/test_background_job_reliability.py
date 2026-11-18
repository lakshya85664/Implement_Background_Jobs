
import pytest

from background_jobs import InvoiceJobInput, JobStatus
from background_job_engine import (
    BackgroundJobAPI,
    FakeDeliveryAdapter,
    InvoicePDFGenerator,
    JobQueue,
    JobStore,
)
from background_job_reliability import (
    FailureClassifier,
    IdempotencyLedger,
    ReliabilityPolicy,
    ReliableBackgroundJobWorker,
)


class FailOnceDeliveryAdapter:
    """Simulate a timeout on the first delivery and success on the next."""

    def __init__(self):
        self.calls = 0
        self.successful_deliveries = 0

    def deliver(self, email: str, pdf_path: str) -> str:
        self.calls += 1

        if self.calls == 1:
            raise TimeoutError("Delivery service timed out")

        self.successful_deliveries += 1
        return "DEL-RETRY-001"


class AlwaysTimeoutDeliveryAdapter:
    """Simulate a dependency that always times out."""

    def __init__(self):
        self.calls = 0

    def deliver(self, email: str, pdf_path: str) -> str:
        self.calls += 1
        raise TimeoutError("Delivery service unavailable")


class PermanentFailureDeliveryAdapter:
    """Simulate a permanent delivery rejection."""

    def __init__(self):
        self.calls = 0

    def deliver(self, email: str, pdf_path: str) -> str:
        self.calls += 1
        raise ValueError("Customer address rejected")


class MalformedDeliveryAdapter:
    """Simulate a malformed response from the delivery service."""

    def deliver(self, email: str, pdf_path: str):
        return None


@pytest.fixture
def invoice():
    return InvoiceJobInput(
        invoice_id="INV-RELIABILITY-001",
        customer_id="CUST-RELIABILITY-001",
        customer_email="customer@example.com",
        amount=3200.00,
        currency="INR",
    )


@pytest.fixture
def system(tmp_path):
    store = JobStore()
    queue = JobQueue()
    api = BackgroundJobAPI(store, queue)

    return store, queue, api, tmp_path


def build_worker(store, queue, tmp_path, adapter, policy=None):
    return ReliableBackgroundJobWorker(
        store=store,
        queue=queue,
        pdf_generator=InvoicePDFGenerator(tmp_path / "pdfs"),
        delivery_adapter=adapter,
        ledger=IdempotencyLedger(),
        policy=policy or ReliabilityPolicy(max_attempts=3),
    )


def test_transient_timeout_is_retried_and_recovers(system, invoice):
    store, queue, api, tmp_path = system
    adapter = FailOnceDeliveryAdapter()
    worker = build_worker(store, queue, tmp_path, adapter)

    accepted = api.submit(invoice)

    first = worker.process_next()

    assert first["status"] == "queued"
    assert first["error_type"] == "transient"
    assert first["next_action"] == "retry"
    assert queue.pending_count == 1

    second = worker.process_next()
    job = store.get(accepted["job_id"])

    assert second["status"] == "completed"
    assert second["attempts"] == 2
    assert job.status == JobStatus.COMPLETED
    assert adapter.calls == 2
    assert adapter.successful_deliveries == 1


def test_retry_limit_leaves_job_failed(system, invoice):
    store, queue, api, tmp_path = system
    adapter = AlwaysTimeoutDeliveryAdapter()
    policy = ReliabilityPolicy(max_attempts=2)
    worker = build_worker(store, queue, tmp_path, adapter, policy)

    accepted = api.submit(invoice)

    first = worker.process_next()
    assert first["next_action"] == "retry"

    second = worker.process_next()
    job = store.get(accepted["job_id"])

    assert second["status"] == "failed"
    assert second["next_action"] == "operator_review"
    assert job.status == JobStatus.FAILED
    assert job.attempts == 2
    assert adapter.calls == 2
    assert queue.pending_count == 0


def test_permanent_failure_is_not_retried(system, invoice):
    store, queue, api, tmp_path = system
    adapter = PermanentFailureDeliveryAdapter()
    worker = build_worker(store, queue, tmp_path, adapter)

    accepted = api.submit(invoice)
    result = worker.process_next()
    job = store.get(accepted["job_id"])

    assert result["error_type"] == "permanent"
    assert result["next_action"] == "operator_review"
    assert job.status == JobStatus.FAILED
    assert job.attempts == 1
    assert adapter.calls == 1
    assert queue.pending_count == 0


def test_malformed_delivery_response_is_rejected(system, invoice):
    store, queue, api, tmp_path = system
    adapter = MalformedDeliveryAdapter()
    worker = build_worker(store, queue, tmp_path, adapter)

    accepted = api.submit(invoice)
    result = worker.process_next()
    job = store.get(accepted["job_id"])

    assert result["status"] == "failed"
    assert result["error_type"] == "permanent"
    assert "Malformed delivery response" in result["error"]
    assert job.status == JobStatus.FAILED
    assert queue.pending_count == 0


def test_duplicate_queue_trigger_does_not_duplicate_delivery(system, invoice):
    store, queue, api, tmp_path = system
    adapter = FakeDeliveryAdapter()
    worker = build_worker(store, queue, tmp_path, adapter)

    accepted = api.submit(invoice)
    job_id = accepted["job_id"]

    # The API already queued this job.
    assert worker.enqueue_once(job_id) is False
    assert queue.pending_count == 1

    worker.process_next()

    # Simulate a duplicate trigger arriving after completion.
    queue.enqueue(job_id)
    duplicate_result = worker.process_next()

    assert duplicate_result["status"] == "completed"
    assert duplicate_result["next_action"] == "no_op"
    assert len(adapter.deliveries) == 1


def test_completed_job_replay_returns_cached_result(system, invoice):
    store, queue, api, tmp_path = system
    adapter = FakeDeliveryAdapter()
    worker = build_worker(store, queue, tmp_path, adapter)

    accepted = api.submit(invoice)
    job_id = accepted["job_id"]

    worker.process_next()
    original_job = store.get(job_id)
    original_delivery_id = original_job.result.delivery_id

    replay_result = worker.replay(job_id)

    assert replay_result["status"] == "completed"
    assert replay_result["next_action"] == "return_cached_result"
    assert replay_result["result"]["delivery_id"] == original_delivery_id
    assert len(adapter.deliveries) == 1


def test_idempotency_ledger_reuses_delivery_id():
    ledger = IdempotencyLedger()

    assert ledger.get_delivery("JOB-1") is None

    ledger.record_delivery("JOB-1", "DEL-1")

    assert ledger.get_delivery("JOB-1") == "DEL-1"


def test_failure_classifier_distinguishes_errors():
    assert FailureClassifier.classify(
        TimeoutError("timeout")
    ) == "transient"

    assert FailureClassifier.classify(
        ConnectionError("connection lost")
    ) == "transient"

    assert FailureClassifier.classify(
        ValueError("invalid response")
    ) == "permanent"
