# backend/app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# this will import the router defined in app/api/metrics.py
from app.api.metrics import router as metrics_router

app = FastAPI(
    title="Carwash Data API",
    version="0.1.0",
    description="API for querying cleaned CryptoPay carwash data stored in SQLite.",
)

# CORS middleware so a future frontend can call this API from the browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # dev: allow all origins; lock down later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register your metrics endpoints under /metrics/...
app.include_router(metrics_router)


@app.get("/", tags=["system"])
def root():
    return {"message": "Carwash Data API is running"}


@app.get("/health", tags=["system"])
def health_check():
    return {"status": "ok"}
