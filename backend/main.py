import uuid
import urllib.parse
import httpx
from pathlib import Path
from datetime import date
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config
from pipeline import run_pipeline
from state import job_store
from events import get_events

app = FastAPI(title="Photo Curator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIRECT_URI = f"{config.APP_BASE_URL}/auth/callback"

GOOGLE_SCOPES = " ".join([
    "https://www.googleapis.com/auth/photoslibrary.readonly",
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
])

token_store: dict = {"access_token": "", "refresh_token": ""}
created_album_ids: set = set()


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
                "client_id": config.GOOGLE_CLIENT_ID,
                "client_secret": config.GOOGLE_CLIENT_SECRET,
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
    return {"connected": bool(token_store.get("access_token"))}


@app.post("/auth/disconnect")
async def auth_disconnect():
    token_store["access_token"] = ""
    token_store["refresh_token"] = ""
    return {"ok": True}


# ── Events (trip/notable day suggestions) ─────────────────────────────────────
@app.get("/api/events")
async def get_event_suggestions(days: int = None):
    access_token = token_store.get("access_token", "")
    if not access_token:
        raise HTTPException(401, "Not connected")
    if days is None:
        days = config.LOOKBACK_DAYS
    days = min(days, config.MAX_LOOKBACK_DAYS)
    events = await get_events(access_token, days)
    return {"events": events, "days_searched": days}


# ── Albums ─────────────────────────────────────────────────────────────────────
@app.get("/api/albums")
async def get_albums():
    access_token = token_store.get("access_token", "")
    if not access_token:
        raise HTTPException(401, "Not connected")

    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(
            "https://photoslibrary.googleapis.com/v1/albums",
            headers=headers,
            params={"pageSize": 20},
        )
        data = r.json()
        if "error" in data:
            raise HTTPException(400, data["error"].get("message", "API error"))

        albums_raw = data.get("albums", [])[:10]
        albums = []

        for album in albums_raw:
            album_id = album.get("id", "")
            cover_url = album.get("coverPhotoBaseUrl", "")
            if cover_url:
                cover_url += "=w400-h300-c"

            date_range = None
            location = None
            try:
                mr = await client.post(
                    "https://photoslibrary.googleapis.com/v1/mediaItems:search",
                    headers=headers,
                    json={"albumId": album_id, "pageSize": 10},
                )
                items = mr.json().get("mediaItems", [])
                if items:
                    times = sorted(
                        t for t in (i.get("mediaMetadata", {}).get("creationTime", "")[:10] for i in items) if t
                    )
                    if times:
                        date_range = times[0] if times[0] == times[-1] else f"{times[0]} – {times[-1]}"

                    for item in items:
                        meta = item.get("mediaMetadata", {})
                        lat = meta.get("photo", {}).get("latitude")
                        lng = meta.get("photo", {}).get("longitude")
                        if lat and lng:
                            geo = await client.get(
                                "https://nominatim.openstreetmap.org/reverse",
                                params={"lat": lat, "lon": lng, "format": "json"},
                                headers={"User-Agent": "PhotoCurator/1.0"},
                            )
                            addr = geo.json().get("address", {})
                            parts = [
                                addr.get("city") or addr.get("town") or addr.get("village"),
                                addr.get("country"),
                            ]
                            location = ", ".join(p for p in parts if p)
                            break
            except Exception:
                pass

            albums.append({
                "id": album_id,
                "title": album.get("title", "Untitled"),
                "cover_url": cover_url,
                "photo_count": album.get("mediaItemsCount", "?"),
                "product_url": album.get("productUrl", ""),
                "date_range": date_range,
                "location": location,
                "created_by_app": album_id in created_album_ids,
            })

    return {"albums": albums}


# ── Curate ─────────────────────────────────────────────────────────────────────
class CurateRequest(BaseModel):
    start_date: date
    end_date: date
    album_name: str = "Best Of Trip"


@app.post("/api/curate")
async def start_curate(req: CurateRequest, background_tasks: BackgroundTasks):
    access_token = token_store.get("access_token", "")
    if not access_token:
        raise HTTPException(401, "Google Photos not connected")
    if req.end_date < req.start_date:
        raise HTTPException(400, "End date must be after start date")

    job_id = str(uuid.uuid4())
    job_store[job_id] = {
        "status": "queued", "stage": "Starting", "progress": 0,
        "total": 0, "kept": 0, "album_url": None, "error": None,
        "album_name": req.album_name,
    }

    background_tasks.add_task(
        run_pipeline, job_id, req.start_date, req.end_date,
        req.album_name, access_token, created_album_ids,
    )
    return {"job_id": job_id}


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in job_store:
        raise HTTPException(404, "Job not found")
    return job_store[job_id]


@app.get("/api/health")
async def health():
    return {"ok": True}


FRONTEND_DIR = Path(__file__).parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
