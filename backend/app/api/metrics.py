# backend/app/api/metrics.py

from fastapi import APIRouter

router = APIRouter(prefix="/metrics", tags=["metrics"])

@router.get("/ping")
def metrics_ping():
    return {"message": "metrics router alive"}