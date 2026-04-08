import os
import uuid
import asyncio
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import shutil

from pipeline import run_pipeline
from state import job_store

app = FastAPI(title="Photo Curator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = Path("/tmp/photo-curator/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@app.post("/api/upload")
async def upload_zip(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    album_name: str = "Best Of Trip",
):
    if not file.filename.endswith(".zip"):
        raise HTTPException(400, "Must be a .zip file (Google Takeout export)")

    job_id = str(uuid.uuid4())
    zip_path = UPLOAD_DIR / f"{job_id}.zip"

    with open(zip_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    job_store[job_id] = {
        "status": "queued",
        "stage": "Uploaded",
        "progress": 0,
        "total": 0,
        "kept": 0,
        "album_url": None,
        "error": None,
        "album_name": album_name,
    }

    background_tasks.add_task(run_pipeline, job_id, zip_path, album_name)

    return {"job_id": job_id}


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in job_store:
        raise HTTPException(404, "Job not found")
    return job_store[job_id]


@app.get("/api/health")
async def health():
    return {"ok": True}


# Serve frontend — must be last
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")

