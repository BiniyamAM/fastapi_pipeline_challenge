import pytest
import asyncio
from app.core.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpenException
from app.services.pipeline import AsyncJobPipeline

@pytest.mark.asyncio
async def test_circuit_breaker_tripping():
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
    assert cb.allow_request() is True

    cb.record_failure()
    assert cb.state == CircuitState.CLOSED

    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.allow_request() is False

    # Wait for recovery timeout
    await asyncio.sleep(0.6)
    assert cb.allow_request() is True
    assert cb.state == CircuitState.HALF_OPEN

    cb.record_success()
    assert cb.state == CircuitState.CLOSED

@pytest.mark.asyncio
async def test_pipeline_successful_execution():
    cb = CircuitBreaker()
    pipeline = AsyncJobPipeline(circuit_breaker=cb)

    async def dummy_task():
        return "ok"

    tasks = [{"id": "t1", "func": dummy_task}]
    results = await pipeline.run_batch(tasks)

    assert len(results) == 1
    assert results[0]["status"] == "success"
    assert results[0]["result"] == "ok"

@pytest.mark.asyncio
async def test_pipeline_retry_and_failure():
    cb = CircuitBreaker(failure_threshold=1)
    pipeline = AsyncJobPipeline(circuit_breaker=cb, max_retries=2)

    async def failing_task():
        raise RuntimeError("Permanent failure")

    tasks = [{"id": "t2", "func": failing_task}]
    results = await pipeline.run_batch(tasks)

    assert len(results) == 1
    assert results[0]["status"] == "failed"
    assert "Permanent failure" in results[0]["error"]