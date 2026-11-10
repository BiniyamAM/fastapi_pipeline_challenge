from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Any
from app.core.circuit_breaker import CircuitBreaker
from app.services.pipeline import AsyncJobPipeline

router = APIRouter()
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
pipeline = AsyncJobPipeline(circuit_breaker=circuit_breaker)

class TaskRequest(BaseModel):
    id: str
    should_fail: bool = False

@router.post("/pipeline/run")
async def run_pipeline_endpoint(tasks: List[TaskRequest]):
    async def make_task_func(fail: bool):
        async def _func():
            if fail:
                raise ValueError("Simulated task failure")
            return "Task completed successfully"
        return _func

    formatted_tasks = [
        {"id": t.id, "func": await make_task_func(t.should_fail)}
        for t in tasks
    ]

    results = await pipeline.run_batch(formatted_tasks)
    return {"results": results, "circuit_state": circuit_breaker.state.value}