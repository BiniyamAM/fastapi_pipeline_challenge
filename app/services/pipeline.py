import asyncio
import logging
from typing import List, Dict, Any, Callable
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException

logger = logging.getLogger(__name__)

class AsyncJobPipeline:
    def __init__(self, circuit_breaker: CircuitBreaker, max_retries: int = 3):
        self.circuit_breaker = circuit_breaker
        self.max_retries = max_retries

    async def execute_task(self, task_id: str, task_func: Callable[[], Any]) -> Dict[str, Any]:
        if not self.circuit_breaker.allow_request():
            raise CircuitBreakerOpenException(f"Circuit breaker is OPEN for task {task_id}")

        attempt = 0
        backoff = 0.1

        while attempt < self.max_retries:
            try:
                # If task_func is a coroutine function or callable returning a coroutine
                if asyncio.iscoroutinefunction(task_func):
                    result = await task_func()
                else:
                    result = task_func()
                
                self.circuit_breaker.record_success()
                return {"task_id": task_id, "status": "success", "result": result}
            except Exception as e:
                attempt += 1
                if attempt >= self.max_retries:
                    self.circuit_breaker.record_failure()
                    raise e
                await asyncio.sleep(backoff)
                backoff *= 2

    async def run_batch(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for task in tasks:
            task_id = task["id"]
            func = task["func"]
            try:
                res = await self.execute_task(task_id, func)
                results.append(res)
            except Exception as e:
                results.append({"task_id": task_id, "status": "failed", "error": str(e)})
        return results