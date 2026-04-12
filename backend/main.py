import os
import uuid
import urllib.parse
import httpx
from pathlib import Path
from datetime import date
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from pipeline import run_pipeline, confirm_and_upload, enhance_photo, score_with_claude
from state import job_store
from events import get_events

app = FastAPI(title="Photo Curator")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REDIRECT_URI = f"{config.APP_BASE_URL}/auth/callback"
GOOGLE_SCOPES = " ".join([
    "https://www.googleapis.com/auth/photoslibrary.readonly",
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
])

# Token store -- seeded from env vars on startup
# Refresh token persisted in GOOGLE_REFRESH_TOKEN env var (set manually after first OAuth)
token_store: dict = {
    "access_token":  os.environ.get("GOOGLE_PHOTOS_TOKEN", ""),
    "refresh_token": os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
}
created_album_ids: set = set()


async def get_valid_token() -> str:
    """
    Return a valid access token, auto-refreshing if needed.
    Access tokens expire after 1hr -- refresh token lasts indefinitely.
    """
    # Try existing access token first
    access_token = token_store.get("access_token", "")
    if access_token:
        # Quick validation -- ping Google
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
            return access_token  # Network error, try with existing token

    # Access token expired or missing -- try refresh
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
    params = {"client_id": config.GOOGLE_CLIENT_ID, "redirect_uri": REDIRECT_URI, "response_type": "code",
              "scope": GOOGLE_SCOPES, "access_type": "offline", "prompt": "consent"}
    return RedirectResponse("https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params))


@app.get("/auth/callback")
async def auth_callback(code: str = None, error: str = None):
    if error: return RedirectResponse(f"/?auth=error&reason={error}")
    if not code: return RedirectResponse("/?auth=error&reason=no_code")
    async with httpx.AsyncClient() as client:
        r = await client.post("https://oauth2.googleapis.com/token", data={
            "code": code, "client_id": config.GOOGLE_CLIENT_ID,
            "client_secret": config.GOOGLE_CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code",
        })
        data = r.json()
    if "access_token" not in data:
        return RedirectResponse("/?auth=error&reason=token_exchange_failed")
    token_store["access_token"] = data["access_token"]
    token_store["refresh_token"] = data.get("refresh_token", token_store.get("refresh_token", ""))
    # NOTE: After first successful OAuth, copy the refresh_token to Railway env var
    # GOOGLE_REFRESH_TOKEN so it survives restarts. Print it to logs for easy copy.
    if data.get("refresh_token"):
        print(f"[AUTH] New refresh token obtained. Add to Railway env: GOOGLE_REFRESH_TOKEN={data['refresh_token'][:20]}...")
    return RedirectResponse("/?auth=success")


@app.get("/auth/status")
async def auth_status():
    token = await get_valid_token()
    return {"connected": bool(token)}


@app.post("/auth/disconnect")
async def auth_disconnect():
    token_store["access_token"] = ""
    token_store["refresh_token"] = ""
    return {"ok": True}


# ── Events ─────────────────────────────────────────────────────────────────────
@app.get("/api/events")
async def get_event_suggestions(days: int = None):
    access_token = await get_valid_token()
    if not access_token: raise HTTPException(401, "Not connected")
    days = min(days or config.LOOKBACK_DAYS, config.MAX_LOOKBACK_DAYS)
    events = await get_events(access_token, days)
    return {"events": events, "days_searched": days}


# ── Albums ─────────────────────────────────────────────────────────────────────
@app.get("/api/albums")
async def get_albums():
    access_token = await get_valid_token()
    if not access_token: raise HTTPException(401, "Not connected")
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get("https://photoslibrary.googleapis.com/v1/albums", headers=headers, params={"pageSize": 20})
        data = r.json()
        if "error" in data: raise HTTPException(400, data["error"].get("message", "API error"))
        albums_raw = data.get("albums", [])[:10]
        albums = []
        for album in albums_raw:
            album_id = album.get("id", "")
            cover_url = (album.get("coverPhotoBaseUrl", "") + "=w400-h300-c") if album.get("coverPhotoBaseUrl") else ""
            date_range = location = None
            try:
                mr = await client.post("https://photoslibrary.googleapis.com/v1/mediaItems:search",
                    headers=headers, json={"albumId": album_id, "pageSize": 10})
                items = mr.json().get("mediaItems", [])
                if items:
                    times = sorted(t for t in (i.get("mediaMetadata", {}).get("creationTime", "")[:10] for i in items) if t)
                    if times: date_range = times[0] if times[0] == times[-1] else f"{times[0]} – {times[-1]}"
                    for item in items:
                        meta = item.get("mediaMetadata", {})
                        lat = meta.get("photo", {}).get("latitude")
                        lng = meta.get("photo", {}).get("longitude")
                        if lat and lng:
                            geo = await client.get("https://nominatim.openstreetmap.org/reverse",
                                params={"lat": lat, "lon": lng, "format": "json"},
                                headers={"User-Agent": "PhotoCurator/1.0"})
                            addr = geo.json().get("address", {})
                            parts = [addr.get("city") or addr.get("town") or addr.get("village"), addr.get("country")]
                            location = ", ".join(p for p in parts if p)
                            break
            except Exception:
                pass
            albums.append({"id": album_id, "title": album.get("title", "Untitled"), "cover_url": cover_url,
                "photo_count": album.get("mediaItemsCount", "?"), "product_url": album.get("productUrl", ""),
                "date_range": date_range, "location": location, "created_by_app": album_id in created_album_ids})
    return {"albums": albums}


# ── Curate ─────────────────────────────────────────────────────────────────────
class CurateRequest(BaseModel):
    start_date: date
    end_date: date
    album_name: str = "Best Of Trip"

@app.post("/api/curate")
async def start_curate(req: CurateRequest, background_tasks: BackgroundTasks):
    access_token = await get_valid_token()
    if not access_token: raise HTTPException(401, "Google Photos not connected")
    if req.end_date < req.start_date: raise HTTPException(400, "End date must be after start date")
    job_id = str(uuid.uuid4())
    job_store[job_id] = {"status": "queued", "stage": "Starting", "progress": 0,
        "total": 0, "kept": 0, "album_url": None, "error": None, "album_name": req.album_name}
    background_tasks.add_task(run_pipeline, job_id, req.start_date, req.end_date,
        req.album_name, access_token, created_album_ids)
    return {"job_id": job_id}

@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in job_store: raise HTTPException(404, "Job not found")
    job = job_store[job_id]
    # Return lightweight status (no full preview payload)
    return {k: v for k, v in job.items() if k not in ("preview", "all_clusters", "work_dir")}


# ── Preview endpoints ──────────────────────────────────────────────────────────
@app.get("/api/preview/{job_id}")
async def get_preview(job_id: str):
    if job_id not in job_store: raise HTTPException(404, "Job not found")
    job = job_store[job_id]
    if job["status"] not in ("preview_ready", "uploading"): raise HTTPException(400, "Preview not ready")
    return job["preview"]

@app.get("/api/preview/{job_id}/thumb/{filename}")
async def get_thumb(job_id: str, filename: str):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    work_dir = Path(job.get("work_dir", ""))
    # Try enhanced first, then original thumb
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
    for day in job["preview"]["days"]:
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

    # Find the photo and its cluster
    target_photo = None
    for day in job["preview"]["days"]:
        if day["date"] == action.day:
            for photo in day["photos"]:
                if photo["id"] == action.photo_id:
                    target_photo = photo
                    break

    if not target_photo: raise HTTPException(404, "Photo not found")

    cluster_idx = target_photo.get("cluster_idx")
    if cluster_idx is None: raise HTTPException(400, "No cluster info")

    cluster = job["all_clusters"][cluster_idx]
    if len(cluster) <= 1: raise HTTPException(400, "No alternatives available")

    # Find next best flattering alternative not already shown
    current_ids = {p["id"] for day in job["preview"]["days"] for p in day["photos"]}
    alternatives = [c for c in cluster[1:] if c["filename"] not in current_ids and c.get("flattering", True)]

    if not alternatives: raise HTTPException(400, "No flattering alternatives available")

    next_best = alternatives[0]
    work_dir = Path(job["work_dir"])

    # Generate thumb for the new photo
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

    # Update photo in preview
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
    for day in job["preview"]["days"]:
        if day["date"] == action.day:
            for photo in day["photos"]:
                if photo["id"] == action.photo_id:
                    target_photo = photo
                    break

    if not target_photo: raise HTTPException(404, "Photo not found")

    work_dir = Path(job["work_dir"])
    photo_path = work_dir / target_photo["filename"]
    if not photo_path.exists(): raise HTTPException(404, "Photo file not found")

    enhance_notes = target_photo.get("enhance_notes") or "auto enhance"
    enhanced_path, description = await enhance_photo(photo_path, enhance_notes)

    # Update thumb
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
    """Re-slice the candidate pool at a different keep ratio."""
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    if job["status"] != "preview_ready": raise HTTPException(400, "Preview not ready")

    ratio = max(0.05, min(0.50, req.ratio))
    total = job["preview"]["total_found"]
    target = max(10, int(total * ratio))

    # Flatten all current photos, re-rank by score, take top N
    all_photos = [p for day in job["preview"]["days"] for p in day["photos"]]
    all_photos.sort(key=lambda p: p["score"], reverse=True)

    # Mark removed/included based on new target
    keep_ids = {p["id"] for p in all_photos[:target]}
    for day in job["preview"]["days"]:
        for photo in day["photos"]:
            if not photo.get("manually_kept") and not photo.get("manually_removed"):
                photo["removed"] = photo["id"] not in keep_ids

    kept = sum(1 for day in job["preview"]["days"] for p in day["photos"] if not p.get("removed"))
    return {"ok": True, "kept": kept}


# ── Confirm upload ─────────────────────────────────────────────────────────────
@app.post("/api/confirm/{job_id}")
async def confirm_album(job_id: str, background_tasks: BackgroundTasks):
    if job_id not in job_store: raise HTTPException(404)
    job = job_store[job_id]
    if job["status"] != "preview_ready": raise HTTPException(400, "Not in preview state")
    access_token = await get_valid_token()
    if not access_token: raise HTTPException(401, "Not connected")
    background_tasks.add_task(confirm_and_upload, job_id, access_token, created_album_ids)
    return {"ok": True}


@app.get("/api/health")
async def health():
    return {"ok": True}


FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
