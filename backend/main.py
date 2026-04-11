import os
import uuid
import urllib.parse
import httpx
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
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

# Google OAuth config
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APP_BASE_URL         = os.environ.get("APP_BASE_URL", "https://photos.toddfriedlich.com")
REDIRECT_URI         = f"{APP_BASE_URL}/auth/callback"

GOOGLE_SCOPES = " ".join([
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
])

# In-memory token store
token_store: dict = {
    "access_token": os.environ.get("GOOGLE_PHOTOS_TOKEN", ""),
    "refresh_token": "",
}


# OAuth endpoints
@app.get("/auth/login")
async def auth_login():
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(500, "GOOGLE_CLIENT_ID not configured")
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return RedirectResponse(url)


@app.get("/auth/callback")
async def auth_callback(code: str = None, error: str = None):
    if error:
        return RedirectResponse(f"/?auth=error&reason={error}")
    if not code:
        return RedirectResponse("/?auth=error&reason=no_code")

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code",
            },
        )
        data = r.json()

    if "access_token" not in data:
        return RedirectResponse("/?auth=error&reason=token_exchange_failed")

    token_store["access_token"] = data["access_token"]
    token_store["refresh_token"] = data.get("refresh_token", "")
    return RedirectResponse("/?auth=success")


@app.get("/auth/status")
async def auth_status():
    has_token = bool(token_store.get("access_token"))
    return {"connected": has_token}


@app.post("/auth/disconnect")
async def auth_disconnect():
    token_store["access_token"] = ""
    token_store["refresh_token"] = ""
    return {"ok": True}


# Upload & pipeline
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

    access_token = token_store.get("access_token", "")
    background_tasks.add_task(run_pipeline, job_id, zip_path, album_name, access_token)

    return {"job_id": job_id}


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in job_store:
        raise HTTPException(404, "Job not found")
    return job_store[job_id]


@app.get("/api/health")
async def health():
    return {"ok": True}


# Serve frontend last
FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
