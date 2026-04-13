import os
import uuid
import urllib.parse
import httpx
from pathlib import Path
from datetime import date
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import shutil

import config
from pipeline import run_pipeline, confirm_and_upload, enhance_photo, pick_best_from_cluster
from state import job_store

APP_VERSION = "v24j"

app = FastAPI(title="Photo Curator")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REDIRECT_URI = f"{config.APP_BASE_URL}/auth/callback"
GOOGLE_SCOPES = " ".join([
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
    "https://www.googleapis.com/auth/photospicker.mediaitems.readonly",
])

UPLOAD_DIR = Path("/tmp/photo-curator/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Per-job SSE event queues -- defined in state.py to avoid circular imports
import asyncio
from typing import AsyncGenerator
from state import get_job_queue, push_job_event, job_event_queues

# Structured in-memory logger
from collections import deque
import datetime

class AppLogger:
    def __init__(self, maxlen=1000):
        self._entries = deque(maxlen=maxlen)
        self._session_id = None

    def _write(self, level: str, msg: str, context: dict = None):
        entry = {
            "ts": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": level,
            "msg": msg,
            "v": "v24j",
        }
        if context:
            entry["ctx"] = context
        if self._session_id:
            entry["session"] = self._session_id
        self._entries.append(entry)
        prefix = f"[{level}]"
        if self._session_id:
            prefix += f"[{self._session_id[:8]}]"
        print(f"{entry['ts']} {prefix} {msg}", flush=True)
        return entry

    def info(self, msg: str, **ctx): return self._write("INFO", msg, ctx or None)
    def warn(self, msg: str, **ctx): return self._write("WARN", msg, ctx or None)
    def error(self, msg: str, **ctx): return self._write("ERROR", msg, ctx or None)
    def score(self, filename: str, score: float, scene: str, flattering: bool, **ctx):
        return self._write("SCORE", f"{filename}: {score} | {scene} | flattering={flattering}", ctx or None)

    def set_session(self, session_id: str):
        self._session_id = session_id

    def clear_session(self):
        self._session_id = None

    def get_entries(self, n: int = 200, level: str = None, session: str = None):
        entries = list(self._entries)
        if level:
            entries = [e for e in entries if e["level"] == level]
        if session:
            entries = [e for e in entries if e.get("session", "").startswith(session)]
        return entries[-n:]

    def get_last_session_summary(self):
        """Summarize the most recent job session for debugging."""
        entries = list(self._entries)
        if not entries:
            return {"error": "No log entries yet"}

        # Find the most recent session
        sessions = list(dict.fromkeys(
            e["session"] for e in entries if "session" in e
        ))
        if not sessions:
            return {"error": "No sessions logged yet"}

        last_session = sessions[-1]
        session_entries = [e for e in entries if e.get("session") == last_session]
        errors = [e for e in session_entries if e["level"] == "ERROR"]
        warnings = [e for e in session_entries if e["level"] == "WARN"]
        scores = [e for e in session_entries if e["level"] == "SCORE"]

        return {
            "session_id": last_session,
            "started": session_entries[0]["ts"] if session_entries else None,
            "ended": session_entries[-1]["ts"] if session_entries else None,
            "total_entries": len(session_entries),
            "errors": [e["msg"] for e in errors],
            "warnings": [e["msg"] for e in warnings],
            "scores_count": len(scores),
            "score_range": {
                "min": min((float(e["msg"].split(":")[1].split("|")[0].strip()) for e in scores), default=None),
                "max": max((float(e["msg"].split(":")[1].split("|")[0].strip()) for e in scores), default=None),
            } if scores else None,
            "last_stage": next(
                (e["msg"] for e in reversed(session_entries) if e["level"] == "INFO" and e["msg"].startswith("[JOB]")),
                None
            ),
        }

logger = AppLogger()

@app.on_event("startup")
async def startup_event():
    logger.info(f"Photo Curator {APP_VERSION} starting up")

# Keep backward compat
def log(msg: str):
    logger.info(msg)

token_store: dict = {
    "access_token": os.environ.get("GOOGLE_PHOTOS_TOKEN", ""),
    "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
}
created_album_ids: set = set()


async def get_valid_token() -> str:
    access_token = token_store.get("access_token", "")
    if access_token:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    "https://photoslibrary.googleapis.com/v1/albums",
                    headers={"Authorization": f"Bearer {access_token}"},
                    params={"pageSize": 1},
                )
                if r.status_code != 401:
                    return access_token
        except Exception:
            return access_token

    refresh_token = token_store.get("refresh_token", "")
    if not refresh_token:
        return ""

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": config.GOOGLE_CLIENT_ID,
                    "client_secret": config.GOOGLE_CLIENT_SECRET,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            data = r.json()
        if "access_token" in data:
            token_store["access_token"] = data["access_token"]
            return data["access_token"]
    except Exception:
        pass
    return ""


# ── OAuth ──────────────────────────────────────────────────────────────────────
@app.get("/auth/login")
async def auth_login():
    if not config.GOOGLE_CLIENT_ID:
        raise HTTPException(500, "GOOGLE_CLIENT_ID not configured")
    params = {
        "client_id": config.GOOGLE_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
    }
    return RedirectResponse("https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params))


@app.get("/auth/callback")
async def auth_callback(code: str = None, error: str = None):
    if error: return RedirectResponse(f"/?auth=error&reason={error}")
    if not code: return RedirectResponse("/?auth=error&reason=no_code")
    async with httpx.AsyncClient() as client:
        r = await client.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": config.GOOGLE_CLIENT_ID,
            "client_secret": config.GOOGLE_CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        })
        data = r.json()
    if "access_token" not in data:
        return RedirectResponse("/?auth=error&reason=token_exchange_failed")
    token_store["access_token"] = data["access_token"]
    token_store["refresh_token"] = data.get("refresh_token", token_store.get("refresh_token", ""))
    if data.get("refresh_token"):
        print(f"[AUTH] New refresh token: GOOGLE_REFRESH_TOKEN={data['refresh_token']}", flush=True)
    return RedirectResponse("/?auth=success")


@app.get("/auth/status")
async def auth_status():
    return {"connected": bool(token_store.get("access_token") or token_store.get("refresh_token"))}


@app.post("/auth/disconnect")
async def auth_disconnect():
    token_store["access_token"] = ""
    token_store["refresh_token"] = ""
    return {"ok": True}


@app.get("/auth/refresh-token")
async def get_refresh_token():
    rt = token_store.get("refresh_token", "")
    if not rt:
        return {"error": "No refresh token. Please reconnect Google Photos first."}
    return {"refresh_token": rt}


# ── Upload ZIP ─────────────────────────────────────────────────────────────────
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
        "status": "queued", "stage": "Uploaded", "progress": 0,
        "total": 0, "kept": 0, "album_url": None, "error": None,
        "album_name": album_name,
    }

    access_token = await get_valid_token()
    logger.set_session(job_id)
    logger.info(f"[JOB] Starting ZIP curation", album=album_name, filename=file.filename)
    background_tasks.add_task(run_pipeline_from_zip, job_id, zip_path, album_name, access_token)
    return {"job_id": job_id}


# ── Preview endpoints ──────────────────────────────────────────────────────────
@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in job_store: raise HTTPException(404, "Job not found")
    job = job_store[job_id]
    return {k: v for k, v in job.items() if k not in ("preview", "all_clusters", "work_dir", "picker_ids")}


@app.get("/api/status/{job_id}/picker-ids")
async def get_picker_ids(job_id: str):
    """Return picker media item IDs for this job -- used by frontend to track uploaded photos."""
    if job_id not in job_store: raise HTTPException(404, "Job not found")
    return {"picker_ids": job_store[job_id].get("picker_ids", [])}


@app.get("/api/preview/{job_id}")
async def get_preview(job_id: str):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    if job["status"] not in ("preview_ready", "uploading"): raise HTTPException(400, "Preview not ready")
    return job["preview"]


@app.get("/api/preview/{job_id}/thumb/{filename}")
async def get_thumb(job_id: str, filename: str):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    work_dir = Path(job.get("work_dir", ""))
    thumb_path = work_dir / "thumbs" / filename
    if not thumb_path.exists():
        thumb_path = work_dir / filename
    if not thumb_path.exists(): raise HTTPException(404)
    return FileResponse(str(thumb_path), media_type="image/jpeg")


class PhotoAction(BaseModel):
    photo_id: str
    day: str


@app.post("/api/preview/{job_id}/remove")
async def remove_photo(job_id: str, action: PhotoAction):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    for day in job["preview"]["tranches"]:
        if day["date"] == action.day:
            for photo in day["photos"]:
                if photo["id"] == action.photo_id:
                    photo["removed"] = True
                    return {"ok": True}
    raise HTTPException(404, "Photo not found")


@app.post("/api/preview/{job_id}/swap")
async def swap_photo(job_id: str, action: PhotoAction):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    target_photo = None
    for day in job["preview"]["tranches"]:
        if day["date"] == action.day:
            for photo in day["photos"]:
                if photo["id"] == action.photo_id:
                    target_photo = photo
                    break
    if not target_photo: raise HTTPException(404)
    cluster_idx = target_photo.get("cluster_idx")
    if cluster_idx is None: raise HTTPException(400, "No cluster info")
    cluster = job["all_clusters"][cluster_idx]
    if len(cluster) <= 1: raise HTTPException(400, "No alternatives available")
    current_ids = {p["id"] for day in job["preview"]["tranches"] for p in day["photos"]}
    alternatives = [c for c in cluster[1:] if c["filename"] not in current_ids and c.get("flattering", True)]
    if not alternatives: raise HTTPException(400, "No flattering alternatives available")
    next_best = alternatives[0]
    work_dir = Path(job["work_dir"])
    try:
        from PIL import Image
        thumbs_dir = work_dir / "thumbs"
        thumb_path = thumbs_dir / next_best["filename"]
        if not thumb_path.exists():
            orig = work_dir / next_best["filename"]
            if orig.exists():
                img = Image.open(orig).convert("RGB")
                img.thumbnail((400, 400))
                img.save(thumb_path, "JPEG", quality=80)
    except Exception:
        pass
    target_photo["id"] = next_best["filename"]
    target_photo["filename"] = next_best["filename"]
    target_photo["score"] = round(next_best.get("score", 5.0), 1)
    target_photo["scene"] = next_best.get("scene", "")
    target_photo["enhance_notes"] = next_best.get("enhance_notes")
    target_photo["has_alternatives"] = len(cluster) > 2
    target_photo["enhanced"] = False
    return {"ok": True, "new_photo": target_photo}


@app.post("/api/preview/{job_id}/enhance")
async def enhance_photo_endpoint(job_id: str, action: PhotoAction):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    target_photo = None
    for day in job["preview"]["tranches"]:
        if day["date"] == action.day:
            for photo in day["photos"]:
                if photo["id"] == action.photo_id:
                    target_photo = photo
                    break
    if not target_photo: raise HTTPException(404)
    work_dir = Path(job["work_dir"])
    photo_path = work_dir / target_photo["filename"]
    if not photo_path.exists(): raise HTTPException(404)
    enhanced_path, description = await enhance_photo(photo_path, target_photo.get("enhance_notes") or "auto enhance")
    try:
        from PIL import Image
        thumbs_dir = work_dir / "thumbs"
        thumb = Image.open(enhanced_path).convert("RGB")
        thumb.thumbnail((400, 400))
        thumb.save(thumbs_dir / target_photo["filename"], "JPEG", quality=80)
    except Exception:
        pass
    target_photo["enhanced"] = True
    target_photo["enhance_description"] = description
    return {"ok": True, "description": description}


class RatioRequest(BaseModel):
    ratio: float

@app.post("/api/preview/{job_id}/ratio")
async def adjust_ratio(job_id: str, req: RatioRequest):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    if job["status"] != "preview_ready": raise HTTPException(400, "Preview not ready")
    ratio = max(0.05, min(0.50, req.ratio))
    total = job["preview"]["total_found"]
    target = max(10, int(total * ratio))
    all_photos = [p for day in job["preview"]["tranches"] for p in day["photos"]]
    all_photos.sort(key=lambda p: p["score"], reverse=True)
    keep_ids = {p["id"] for p in all_photos[:target]}
    for day in job["preview"]["tranches"]:
        for photo in day["photos"]:
            if not photo.get("manually_kept") and not photo.get("manually_removed"):
                photo["removed"] = photo["id"] not in keep_ids
    kept = sum(1 for day in job["preview"]["tranches"] for p in day["photos"] if not p.get("removed"))
    return {"ok": True, "kept": kept}


class ConfirmRequest(BaseModel):
    existing_album_id: str = ""
    existing_album_url: str = ""
    already_uploaded_ids: list = []  # Picker IDs already in the album from previous days

@app.post("/api/confirm/{job_id}")
async def confirm_album(job_id: str, req: ConfirmRequest, background_tasks: BackgroundTasks):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    if job["status"] != "preview_ready": raise HTTPException(400, "Not in preview state")
    access_token = await get_valid_token()
    background_tasks.add_task(
        confirm_and_upload, job_id, access_token, created_album_ids,
        req.existing_album_id or None,
        req.existing_album_url or None,
        req.already_uploaded_ids or [],
    )
    return {"ok": True}


# ── Google Photos Picker API ───────────────────────────────────────────────────
@app.post("/api/picker/session")
async def create_picker_session():
    """Create a new Picker API session and return the pickerUri."""
    access_token = await get_valid_token()
    if not access_token:
        raise HTTPException(401, "Not connected")

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://photospicker.googleapis.com/v1/sessions",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={},
        )
        data = r.json()

    if "error" in data:
        raise HTTPException(400, data["error"].get("message", "Failed to create picker session"))

    return {
        "session_id": data.get("id"),
        "picker_uri": data.get("pickerUri"),
        "expires_at": data.get("expireTime"),
    }


@app.get("/api/picker/session/{session_id}")
async def poll_picker_session(session_id: str):
    """Poll a picker session to check if user has finished selecting."""
    access_token = await get_valid_token()
    if not access_token:
        raise HTTPException(401, "Not connected")

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            f"https://photospicker.googleapis.com/v1/sessions/{session_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        data = r.json()

    if "error" in data:
        raise HTTPException(400, data["error"].get("message", "Session error"))

    return {
        "media_items_set": data.get("mediaItemsSet", False),
        "poll_interval": data.get("pollingConfig", {}).get("pollInterval", "10s"),
        "expires_at": data.get("expireTime"),
    }


@app.post("/api/picker/curate")
async def curate_from_picker(
    background_tasks: BackgroundTasks,
    session_id: str,
    album_name: str = "Best Of Trip",
):
    """Start curation pipeline using photos selected via Picker API."""
    access_token = await get_valid_token()
    if not access_token:
        raise HTTPException(401, "Not connected")

    job_id = str(uuid.uuid4())
    job_store[job_id] = {
        "status": "queued", "stage": "Starting", "progress": 0,
        "total": 0, "kept": 0, "album_url": None, "error": None,
        "album_name": album_name,
    }

    logger.set_session(job_id)
    logger.info(f"[JOB] Starting picker curation", album=album_name, photos_selected="pending")
    get_job_queue(job_id)  # Pre-create queue so stream endpoint can connect immediately
    background_tasks.add_task(
        run_pipeline_from_picker,
        job_id, session_id, album_name, access_token
    )
    return {"job_id": job_id}


@app.get("/api/logs")
async def get_logs(n: int = 200, level: str = None, session: str = None):
    """Return structured log entries for debugging."""
    entries = logger.get_entries(n=n, level=level, session=session)
    return {"entries": entries, "count": len(entries)}


@app.get("/api/logs/summary")
async def get_log_summary():
    """Return a human-readable summary of the last run -- great for debugging failures."""
    summary = logger.get_last_session_summary()
    summary["app_version"] = APP_VERSION
    return summary


@app.get("/api/logs/errors")
async def get_errors(n: int = 50):
    """Return only error entries across all sessions."""
    entries = logger.get_entries(n=n, level="ERROR")
    return {"errors": entries, "count": len(entries)}


class FrontendLogEntry(BaseModel):
    level: str = "ERROR"
    msg: str
    ctx: dict = {}

@app.post("/api/logs/frontend")
async def log_frontend(entry: FrontendLogEntry):
    """Receive frontend errors and store in the same log buffer."""
    if entry.level == "ERROR":
        logger.error(f"[FRONTEND] {entry.msg}", **entry.ctx)
    else:
        logger.info(f"[FRONTEND] {entry.msg}", **entry.ctx)
    return {"ok": True}


@app.get("/api/albums")
async def get_albums():
    """Return all albums (curated and linked) from manifests."""
    from pipeline import get_all_album_manifests
    albums = get_all_album_manifests()
    return {"albums": albums}


class LinkedAlbumRequest(BaseModel):
    album_url: str
    album_name: str
    description: str = ""
    cover_url: str = ""


@app.post("/api/albums/link")
async def link_album(req: LinkedAlbumRequest):
    """Manually link an existing Google Photos album."""
    from pipeline import create_linked_album
    if not req.album_url.startswith("https://photos.google.com"):
        raise HTTPException(400, "Must be a Google Photos URL")
    result = create_linked_album(req.album_url, req.album_name, req.description)
    if "error" in result:
        raise HTTPException(409, result["error"])
    return result


class UpdateAlbumRequest(BaseModel):
    album_name: str = ""
    description: str = ""
    cover_url: str = ""


@app.patch("/api/albums/{album_id}")
async def update_album(album_id: str, req: UpdateAlbumRequest):
    """Update metadata for a linked album."""
    from pipeline import update_linked_album
    result = update_linked_album(
        album_id,
        album_name=req.album_name or None,
        description=req.description or None,
        cover_url=req.cover_url or None,
    )
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.delete("/api/albums/{album_id}")
async def delete_album(album_id: str):
    """Remove an album from the browse page (does not delete from Google Photos)."""
    from pipeline import delete_album_manifest
    result = delete_album_manifest(album_id)
    if "error" in result:
        raise HTTPException(404, result["error"])
    return result


@app.get("/api/stream/{job_id}")
async def stream_job(job_id: str):
    """SSE endpoint -- streams real-time pipeline events to the frontend."""
    from fastapi.responses import StreamingResponse
    import json

    # Create queue for this job if it doesn't exist
    queue = get_job_queue(job_id)

    async def event_generator() -> AsyncGenerator[str, None]:
        # Send any existing job state immediately
        if job_id in job_store:
            job = job_store[job_id]
            init_data = {"type": "status", "stage": job.get("stage", ""), "progress": job.get("progress", 0), "total": job.get("total", 0)}
            yield "data: " + json.dumps(init_data) + "\n\n"

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                yield "data: " + json.dumps(event) + "\n\n"
                if event.get("type") in ("done", "error", "preview_ready"):
                    break
            except asyncio.TimeoutError:
                yield 'data: {"type": "ping"}\n\n'
            except Exception:
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/api/health")
async def health():
    return {"ok": True, "version": APP_VERSION}


from fastapi.responses import HTMLResponse

FRONTEND_DIR = Path(__file__).parent / "frontend"

@app.get("/browse", response_class=HTMLResponse)
async def browse_page():
    browse_path = FRONTEND_DIR / "browse.html"
    if browse_path.exists():
        return HTMLResponse(browse_path.read_text())
    return HTMLResponse("<h1>Browse page not found</h1>", status_code=404)

if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


# ── ZIP pipeline wrapper ───────────────────────────────────────────────────────
async def run_pipeline_from_zip(job_id: str, zip_path: Path, album_name: str, access_token: str):
    """Extract ZIP and run the full curation pipeline."""
    import zipfile
    from pipeline import (
        is_screenshot, blur_score, exposure_score, perceptual_hash,
        get_exif_datetime, cluster_duplicates, score_with_claude,
        generate_trip_narrative, upload_to_google_photos, confirm_and_upload,
        update_job, WORK_DIR
    )
    from state import job_store
    import shutil

    work_dir = WORK_DIR / job_id
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        thumbs_dir = work_dir / "thumbs"
        thumbs_dir.mkdir(exist_ok=True)

        # 1. Extract ZIP
        update_job(job_id, status="running", stage="Extracting ZIP", progress=5)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(work_dir)

        # 2. Collect images
        IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
        image_paths = [f for f in work_dir.rglob("*")
                      if f.suffix.lower() in IMAGE_EXTENSIONS and f.is_file()
                      and "thumbs" not in str(f)]
        total_found = len(image_paths)
        update_job(job_id, total=total_found, stage=f"Found {total_found} photos", progress=15)

        if total_found == 0:
            update_job(job_id, status="error", error="No images found in ZIP")
            return

        # 3. Cheap filters
        update_job(job_id, stage=f"Analyzing {total_found} photos", progress=20)
        images = []
        for i, path in enumerate(image_paths):
            if is_screenshot(path): continue
            ct = get_exif_datetime(path)
            day = ct.strftime("%Y-%m-%d") if ct else "unknown"
            images.append({
                "path": path, "blur": blur_score(path), "exposure": exposure_score(path),
                "hash": perceptual_hash(path), "dt": ct, "day": day,
                "filename": path.name, "item_id": path.stem, "claude_result": None,
            })
            if i % 50 == 0:
                pct = 20 + int((i / total_found) * 15)
                update_job(job_id, progress=pct, stage=f"Analyzing photos ({i}/{total_found})")

        images = [img for img in images
                  if img["blur"] > config.MIN_BLUR_SCORE and img["exposure"] > config.MIN_EXPOSURE_SCORE]

        # 4. Cluster
        update_job(job_id, stage="Clustering duplicates", progress=36)
        clusters = cluster_duplicates(images)
        for cluster in clusters:
            for img in cluster:
                img["cheap_score"] = img["blur"] * 0.6 + img["exposure"] * 0.4
            cluster.sort(key=lambda x: x["cheap_score"], reverse=True)

        # 5. Claude scoring
        # Score top 2 candidates per cluster (best cheap scores)
        # This prevents good photos being discarded because cluster[0] happened to score poorly
        candidates = []
        seen_paths = set()
        for cluster_idx, c in enumerate(clusters):
            for member in c[:2]:  # safe -- slicing never raises IndexError
                if member["path"] not in seen_paths:
                    member["cluster_idx"] = cluster_idx  # assign here, not during scoring
                    member["cluster_size"] = len(c)
                    candidates.append(member)
                    seen_paths.add(member["path"])
        update_job(job_id, stage=f"AI scoring {len(candidates)} candidates", progress=42)
        scored = []
        for i, img in enumerate(candidates):
            result = await score_with_claude(img["path"])
            img["claude_result"] = result
            img["score"] = result.get("score", 5.0)
            img["flattering"] = result.get("flattering", True)
            img["scene"] = result.get("scene", "")
            img["enhance_notes"] = result.get("enhance_notes")
            scored.append(img)
            if i % 10 == 0:
                pct = 42 + int((i / len(candidates)) * 30)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Select keepers using smart cluster picking
        scored_by_cluster = {}
        for img in scored:
            idx = img["cluster_idx"]
            if idx not in scored_by_cluster:
                scored_by_cluster[idx] = []
            scored_by_cluster[idx].append(img)

        pre_keepers = []
        for idx, cluster_members in scored_by_cluster.items():
            picks = pick_best_from_cluster(cluster_members, max_keep=2)
            pre_keepers.extend(picks)

        flattering = [img for img in pre_keepers if img.get("flattering", True) and img.get("score", 5.0) >= config.MIN_SCORE]
        flattering.sort(key=lambda x: x["score"], reverse=True)
        # target is % of total input photos, not scored candidates
        target_count = max(10, int(total_found * config.TARGET_KEEP_RATIO))
        keepers = flattering[:target_count]

        # 7. Thumbnails
        update_job(job_id, stage="Generating preview thumbnails", progress=74)
        for img in keepers:
            try:
                from PIL import Image
                thumb = Image.open(img["path"]).convert("RGB")
                thumb.thumbnail((400, 400))
                thumb_path = thumbs_dir / img["filename"]
                thumb.save(thumb_path, "JPEG", quality=80)
                img["thumb"] = img["filename"]
            except Exception:
                img["thumb"] = None

        # 8. Trip narrative
        update_job(job_id, stage="Writing trip narrative", progress=80)
        days_map = {}
        for img in keepers:
            d = img["day"]
            if d not in days_map: days_map[d] = []
            days_map[d].append(img)

        days_summary = []
        for d in sorted(days_map.keys()):
            scenes = list(set(img["scene"] for img in days_map[d] if img.get("scene")))
            days_summary.append({"date": d, "photo_count": len(days_map[d]),
                                  "scenes": ", ".join(scenes[:5]), "locations": None})

        day_names = await generate_trip_narrative(days_summary, album_name)

        # 9. Build preview
        preview_days = []
        for d in sorted(days_map.keys()):
            day_photos = []
            for img in days_map[d]:
                day_photos.append({
                    "id": img["filename"], "filename": img["filename"],
                    "score": round(img["score"], 1), "scene": img.get("scene", ""),
                    "enhance_notes": img.get("enhance_notes"), "has_alternatives": img["cluster_size"] > 1,
                    "enhanced": False, "removed": False, "cluster_idx": img["cluster_idx"],
                })
            preview_days.append({
                "date": d, "day_name": day_names.get(d, d),
                "photos_found": len([img for img in scored if img["day"] == d]),
                "photos": day_photos,
            })

        all_clusters_serializable = []
        for i, cluster in enumerate(clusters):
            all_clusters_serializable.append([{
                "filename": img["filename"], "day": img["day"],
                "score": img.get("score", img.get("cheap_score", 0)),
                "flattering": img.get("flattering", True),
                "scene": img.get("scene", ""), "enhance_notes": img.get("enhance_notes"),
            } for img in cluster])

        update_job(job_id, status="preview_ready", stage="Preview ready", progress=90,
                   kept=len(keepers),
                   preview={"album_name": album_name, "total_found": total_found, "tranches": preview_days},
                   all_clusters=all_clusters_serializable, work_dir=str(work_dir))

    except Exception as e:
        update_job(job_id, status="error", error=str(e))
        if work_dir.exists(): shutil.rmtree(work_dir)
    finally:
        if zip_path.exists(): zip_path.unlink()


# ── Picker pipeline ────────────────────────────────────────────────────────────
async def run_pipeline_from_picker(job_id: str, session_id: str, album_name: str, access_token: str):
    """Fetch photos from Picker session and run the curation pipeline."""
    import asyncio
    from pipeline import (
        is_screenshot, blur_score, exposure_score, perceptual_hash,
        get_exif_datetime, cluster_duplicates, score_with_claude,
        generate_trip_narrative, upload_to_google_photos,
        update_job, WORK_DIR
    )
    import shutil

    work_dir = WORK_DIR / job_id
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        thumbs_dir = work_dir / "thumbs"
        thumbs_dir.mkdir(exist_ok=True)

        # 1. Fetch media items from picker session
        update_job(job_id, status="running", stage="Fetching selected photos", progress=5)
        headers = {"Authorization": f"Bearer {access_token}"}
        items = []
        next_page_token = None

        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                params = {"sessionId": session_id, "pageSize": 100}
                if next_page_token:
                    params["pageToken"] = next_page_token
                r = await client.get(
                    "https://photospicker.googleapis.com/v1/mediaItems",
                    headers=headers,
                    params=params,
                )
                data = r.json()
                if "error" in data:
                    raise Exception(f"Picker API error: {data['error'].get('message', str(data['error']))}")
                batch = data.get("mediaItems", [])
                items.extend(batch)
                update_job(job_id, stage=f"Fetching photos ({len(items)} found)", progress=8)
                next_page_token = data.get("nextPageToken")
                if not next_page_token:
                    break
                await asyncio.sleep(0.1)

        # Filter videos, keep only photos
        items = [i for i in items if i.get("type") == "PHOTO" or
                 (i.get("mediaFile", {}).get("mimeType", "").startswith("image/"))]

        total_found = len(items)
        update_job(job_id, total=total_found, stage=f"Found {total_found} photos — downloading", progress=10)

        if total_found == 0:
            update_job(job_id, status="error", error="No photos found in selection")
            return

        # 2. Download photos
        semaphore = asyncio.Semaphore(8)

        async def download_one(item, idx):
            async with semaphore:
                try:
                    # Picker API uses mediaFile.baseUrl
                    base_url = item.get("mediaFile", {}).get("baseUrl") or item.get("baseUrl", "")
                    if not base_url:
                        return None
                    # Append =d for full download
                    url = base_url + "=d"
                    filename = item.get("mediaFile", {}).get("filename") or f"photo_{idx}.jpg"
                    dest = work_dir / filename
                    if dest.exists():
                        dest = work_dir / f"{dest.stem}_{idx}{dest.suffix}"

                    async with httpx.AsyncClient(timeout=60) as client:
                        r = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
                        if r.status_code == 200:
                            dest.write_bytes(r.content)
                            if idx % 25 == 0:
                                pct = 10 + int((idx / total_found) * 22)
                                update_job(job_id, stage=f"Downloading photos ({idx}/{total_found})", progress=pct)
                            return dest, item.get("id", "")
                except Exception:
                    pass
                return None, ""

        tasks = [download_one(item, i) for i, item in enumerate(items)]
        results = await asyncio.gather(*tasks)
        # Store picker ID → path mapping for dedup checking
        picker_id_map = {picker_id: path for path, picker_id in results if path is not None and picker_id}
        image_paths = [path for path, _ in results if path is not None]

        if not image_paths:
            update_job(job_id, status="error", error="Failed to download any photos")
            return

        # 3. Cheap filters
        update_job(job_id, stage=f"Analyzing {len(image_paths)} photos", progress=34)
        images = []
        for i, path in enumerate(image_paths):
            if is_screenshot(path): continue
            ct = get_exif_datetime(path)
            day = ct.strftime("%Y-%m-%d") if ct else "unknown"
            images.append({
                "path": path, "blur": blur_score(path), "exposure": exposure_score(path),
                "hash": perceptual_hash(path), "dt": ct, "day": day,
                "filename": path.name, "item_id": path.stem, "claude_result": None,
            })
            if i % 50 == 0:
                pct = 34 + int((i / len(image_paths)) * 10)
                update_job(job_id, progress=pct, stage=f"Analyzing photos ({i}/{len(image_paths)})")

        images = [img for img in images
                  if img["blur"] > config.MIN_BLUR_SCORE and img["exposure"] > config.MIN_EXPOSURE_SCORE]

        # 4. Cluster
        update_job(job_id, stage="Clustering duplicates", progress=46)
        clusters = cluster_duplicates(images)
        for cluster in clusters:
            for img in cluster:
                img["cheap_score"] = img["blur"] * 0.6 + img["exposure"] * 0.4
            cluster.sort(key=lambda x: x["cheap_score"], reverse=True)

        # 5. Claude scoring
        # Score top 2 candidates per cluster (best cheap scores)
        # This prevents good photos being discarded because cluster[0] happened to score poorly
        candidates = []
        seen_paths = set()
        for cluster_idx, c in enumerate(clusters):
            for member in c[:2]:
                if member["path"] not in seen_paths:
                    member["cluster_idx"] = cluster_idx
                    member["cluster_size"] = len(c)
                    candidates.append(member)
                    seen_paths.add(member["path"])
        update_job(job_id, stage=f"AI scoring {len(candidates)} candidates", progress=50)
        scored = []
        for i, img in enumerate(candidates):
            result = await score_with_claude(img["path"])
            img["claude_result"] = result
            img["score"] = result.get("score", 5.0)
            img["flattering"] = result.get("flattering", True)
            img["scene"] = result.get("scene", "")
            img["enhance_notes"] = result.get("enhance_notes")
            scored.append(img)
            if i % 10 == 0:
                pct = 50 + int((i / len(candidates)) * 26)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Select keepers using smart cluster picking
        scored_by_cluster = {}
        for img in scored:
            idx = img["cluster_idx"]
            if idx not in scored_by_cluster:
                scored_by_cluster[idx] = []
            scored_by_cluster[idx].append(img)

        pre_keepers = []
        for idx, cluster_members in scored_by_cluster.items():
            picks = pick_best_from_cluster(cluster_members, max_keep=2)
            pre_keepers.extend(picks)

        flattering = [img for img in pre_keepers if img.get("flattering", True) and img.get("score", 5.0) >= config.MIN_SCORE]
        flattering.sort(key=lambda x: x["score"], reverse=True)
        # target is % of total input photos, not scored candidates
        target_count = max(10, int(total_found * config.TARGET_KEEP_RATIO))
        keepers = flattering[:target_count]

        # 7. Thumbnails
        update_job(job_id, stage="Generating preview thumbnails", progress=78)
        for img in keepers:
            try:
                from PIL import Image
                thumb = Image.open(img["path"]).convert("RGB")
                thumb.thumbnail((400, 400))
                thumb_path = thumbs_dir / img["filename"]
                thumb.save(thumb_path, "JPEG", quality=80)
                img["thumb"] = img["filename"]
            except Exception:
                img["thumb"] = None

        # 8. Trip narrative
        update_job(job_id, stage="Writing trip narrative", progress=82)
        days_map = {}
        for img in keepers:
            d = img["day"]
            if d not in days_map: days_map[d] = []
            days_map[d].append(img)

        days_summary = [{"date": d, "photo_count": len(days_map[d]),
                          "scenes": ", ".join(list(set(img["scene"] for img in days_map[d] if img.get("scene")))[:5]),
                          "locations": None}
                         for d in sorted(days_map.keys())]
        day_names = await generate_trip_narrative(days_summary, album_name)

        # 9. Build preview
        preview_days = []
        for d in sorted(days_map.keys()):
            day_photos = [{"id": img["filename"], "filename": img["filename"],
                           "score": round(img["score"], 1), "scene": img.get("scene", ""),
                           "enhance_notes": img.get("enhance_notes"), "has_alternatives": img["cluster_size"] > 1,
                           "enhanced": False, "removed": False, "cluster_idx": img["cluster_idx"]}
                          for img in days_map[d]]
            preview_days.append({"date": d, "day_name": day_names.get(d, d),
                                  "photos_found": len([img for img in scored if img["day"] == d]),
                                  "photos": day_photos})

        all_clusters_serializable = [[{"filename": img["filename"], "day": img["day"],
                                        "score": img.get("score", img.get("cheap_score", 0)),
                                        "flattering": img.get("flattering", True),
                                        "scene": img.get("scene", ""), "enhance_notes": img.get("enhance_notes")}
                                       for img in cluster] for cluster in clusters]

        # Delete picker session
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.delete(
                    f"https://photospicker.googleapis.com/v1/sessions/{session_id}",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
        except Exception:
            pass

        # Store picker IDs so frontend can track what's already been uploaded
        # picker_id_map: picker_id -> path; we need filename -> picker_id for dedup
        filename_to_picker_id = {}
        if 'picker_id_map' in locals():
            filename_to_picker_id = {path.name: pid for pid, path in picker_id_map.items()}

        update_job(job_id, status="preview_ready", stage="Preview ready", progress=90,
                   kept=len(keepers),
                   picker_ids=list(filename_to_picker_id.values()),
                   preview={"album_name": album_name, "total_found": total_found, "tranches": preview_days},
                   all_clusters=all_clusters_serializable,
                   picker_id_map=filename_to_picker_id,
                   work_dir=str(work_dir))

    except Exception as e:
        update_job(job_id, status="error", error=str(e))
        if work_dir.exists():
            shutil.rmtree(work_dir)
