from fastapi import FastAPI
from app.api.endpoints import router

app = FastAPI(title="Async Pipeline & Circuit Breaker Challenge")

app.include_router(router)