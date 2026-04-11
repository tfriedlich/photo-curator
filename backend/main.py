import os
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

from pipeline import run_pipeline
from state import job_store

app = FastAPI(title="Photo Curator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APP_BASE_URL         = os.environ.get("APP_BASE_URL", "https://photos.toddfriedlich.com")
REDIRECT_URI         = f"{APP_BASE_URL}/auth/callback"

GOOGLE_SCOPES = " ".join([
    "https://www.googleapis.com/auth/photoslibrary.readonly",
    "https://www.googleapis.com/auth/photoslibrary",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
])

token_store: dict = {
    "access_token": os.environ.get("GOOGLE_PHOTOS_TOKEN", ""),
    "refresh_token": "",
}

# Track album IDs created by this app
created_album_ids: set = set()


# ── OAuth ──────────────────────────────────────────────────────────────────────
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
    return {"connected": bool(token_store.get("access_token"))}


@app.post("/auth/disconnect")
async def auth_disconnect():
    token_store["access_token"] = ""
    token_store["refresh_token"] = ""
    return {"ok": True}


# ── Albums endpoint ────────────────────────────────────────────────────────────
@app.get("/api/albums")
async def get_albums():
    """Return 10 most recent albums, flagging ones created by this app."""
    access_token = token_store.get("access_token", "")
    if not access_token:
        raise HTTPException(401, "Not connected")

    headers = {"Authorization": f"Bearer {access_token}"}

    async with httpx.AsyncClient(timeout=30) as client:
        # Fetch albums list
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

            # Fetch a few media items to get date range + location
            date_range = None
            location = None
            try:
                mr = await client.post(
                    "https://photoslibrary.googleapis.com/v1/mediaItems:search",
                    headers=headers,
                    json={"albumId": album_id, "pageSize": 10},
                )
                media_data = mr.json()
                items = media_data.get("mediaItems", [])

                if items:
                    # Date range from metadata
                    times = []
                    for item in items:
                        ct = item.get("mediaMetadata", {}).get("creationTime", "")
                        if ct:
                            times.append(ct[:10])  # YYYY-MM-DD
                    if times:
                        times.sort()
                        if times[0] == times[-1]:
                            date_range = times[0]
                        else:
                            date_range = f"{times[0]} – {times[-1]}"

                    # Location from first item that has GPS
                    for item in items:
                        meta = item.get("mediaMetadata", {})
                        lat = meta.get("photo", {}).get("latitude") or meta.get("latitude")
                        lng = meta.get("photo", {}).get("longitude") or meta.get("longitude")
                        if lat and lng:
                            # Reverse geocode
                            geo = await client.get(
                                f"https://nominatim.openstreetmap.org/reverse",
                                params={"lat": lat, "lon": lng, "format": "json"},
                                headers={"User-Agent": "PhotoCurator/1.0"},
                            )
                            geo_data = geo.json()
                            addr = geo_data.get("address", {})
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


# ── Curate endpoint ────────────────────────────────────────────────────────────
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
        "status": "queued",
        "stage": "Starting",
        "progress": 0,
        "total": 0,
        "kept": 0,
        "album_url": None,
        "album_id": None,
        "error": None,
        "album_name": req.album_name,
    }

    background_tasks.add_task(
        run_pipeline,
        job_id,
        req.start_date,
        req.end_date,
        req.album_name,
        access_token,
        created_album_ids,
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
