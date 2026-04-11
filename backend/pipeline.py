import os
import shutil
import base64
import asyncio
from pathlib import Path
from datetime import datetime, date
from typing import Optional
import httpx

from state import job_store

# ── constants ──────────────────────────────────────────────────────────────────
WORK_DIR = Path("/tmp/photo-curator/jobs")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
TARGET_KEEP_RATIO = 0.20
DUPLICATE_HASH_THRESHOLD = 10
BURST_WINDOW_SECONDS = 3
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_PHOTOS_PAGE_SIZE = 100  # max allowed by API


def update_job(job_id: str, **kwargs):
    job_store[job_id].update(kwargs)


# ── stage 1: fetch from Google Photos API ─────────────────────────────────────
async def fetch_media_items(
    access_token: str,
    start_date: date,
    end_date: date,
    job_id: str,
) -> list[dict]:
    """
    Page through Google Photos mediaItems.search for a date range.
    Returns list of {id, filename, baseUrl, mimeType, creationTime}.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    items = []
    next_page_token = None

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body = {
                "pageSize": GOOGLE_PHOTOS_PAGE_SIZE,
                "filters": {
                    "dateFilter": {
                        "ranges": [{
                            "startDate": {"year": start_date.year, "month": start_date.month, "day": start_date.day},
                            "endDate":   {"year": end_date.year,   "month": end_date.month,   "day": end_date.day},
                        }]
                    },
                    "mediaTypeFilter": {"mediaTypes": ["PHOTO"]},
                    "includeArchivedMedia": True,
                },
            }
            if next_page_token:
                body["pageToken"] = next_page_token

            r = await client.post(
                "https://photoslibrary.googleapis.com/v1/mediaItems:search",
                headers=headers,
                json=body,
            )
            data = r.json()

            if "error" in data:
                raise Exception(f"Google Photos API error: {data['error'].get('message', str(data['error']))}")

            batch = data.get("mediaItems", [])
            items.extend(batch)

            update_job(job_id, stage=f"Fetching photo list... ({len(items)} found)", progress=8)

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            # Respect rate limits
            await asyncio.sleep(0.1)

    return items


async def download_photo(
    item: dict,
    dest_dir: Path,
    access_token: str,
    client: httpx.AsyncClient,
) -> Optional[Path]:
    """Download a single photo to dest_dir. Returns path or None on failure."""
    try:
        # baseUrl must have =d appended to get full original download
        url = item["baseUrl"] + "=d"
        filename = item.get("filename", item["id"] + ".jpg")
        dest = dest_dir / filename

        # Avoid filename collisions
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            dest = dest_dir / f"{stem}_{item['id'][:8]}{suffix}"

        r = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code == 200:
            dest.write_bytes(r.content)
            return dest
    except Exception:
        pass
    return None


async def download_all_photos(
    items: list[dict],
    dest_dir: Path,
    access_token: str,
    job_id: str,
) -> list[Path]:
    """Download all photos with concurrency limit to avoid rate limiting."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    semaphore = asyncio.Semaphore(8)  # max 8 concurrent downloads

    async def bounded_download(item, idx):
        async with semaphore:
            async with httpx.AsyncClient(timeout=60) as client:
                path = await download_photo(item, dest_dir, access_token, client)
                if idx % 25 == 0:
                    pct = 10 + int((idx / len(items)) * 25)
                    update_job(job_id, stage=f"Downloading photos ({idx}/{len(items)})", progress=pct)
                return path

    tasks = [bounded_download(item, i) for i, item in enumerate(items)]
    results = await asyncio.gather(*tasks)
    paths = [p for p in results if p is not None]
    return paths


# ── stage 2: cheap filters ─────────────────────────────────────────────────────
def is_screenshot(path: Path) -> bool:
    name = path.name.lower()
    return "screenshot" in name or "screen shot" in name or "screen_shot" in name


def get_exif_datetime(path: Path) -> Optional[datetime]:
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
        img = Image.open(path)
        exif_data = img._getexif()
        if not exif_data:
            return None
        for tag_id, value in exif_data.items():
            tag = TAGS.get(tag_id, tag_id)
            if tag == "DateTimeOriginal":
                return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except Exception:
        pass
    return None


def blur_score(path: Path) -> float:
    try:
        import cv2
        img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
        if img is None:
            return 0.0
        h, w = img.shape
        if max(h, w) > 1000:
            scale = 1000 / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))
        return float(cv2.Laplacian(img, cv2.CV_64F).var())
    except Exception:
        return 0.0


def exposure_score(path: Path) -> float:
    try:
        from PIL import Image
        import numpy as np
        img = Image.open(path).convert("L").resize((200, 200))
        arr = np.array(img, dtype=float)
        mean = arr.mean()
        if mean < 30:
            return 0.1
        if mean > 240:
            return 0.2
        return 1.0 - abs(mean - 128) / 128
    except Exception:
        return 0.5


def perceptual_hash(path: Path):
    try:
        import imagehash
        from PIL import Image
        img = Image.open(path).convert("RGB")
        return imagehash.phash(img)
    except Exception:
        return None


# ── stage 3: clustering ────────────────────────────────────────────────────────
def cluster_duplicates(images: list[dict]) -> list[list[dict]]:
    clusters = []
    used = set()

    for i, img in enumerate(images):
        if i in used:
            continue
        cluster = [img]
        used.add(i)

        for j, other in enumerate(images):
            if j in used or j == i:
                continue
            if img["hash"] and other["hash"]:
                try:
                    if img["hash"] - other["hash"] <= DUPLICATE_HASH_THRESHOLD:
                        cluster.append(other)
                        used.add(j)
                        continue
                except Exception:
                    pass
            if img["dt"] and other["dt"]:
                delta = abs((img["dt"] - other["dt"]).total_seconds())
                if delta <= BURST_WINDOW_SECONDS:
                    cluster.append(other)
                    used.add(j)

        clusters.append(cluster)

    return clusters


def pick_best_cheap(cluster: list[dict]) -> dict:
    return max(cluster, key=lambda x: x["blur"] * 0.6 + x["exposure"] * 0.4)


# ── stage 4: claude vision scoring ────────────────────────────────────────────
async def score_with_claude(path: Path) -> float:
    if not ANTHROPIC_API_KEY:
        return 5.0
    try:
        from PIL import Image
        import io
        img = Image.open(path).convert("RGB")
        img.thumbnail((1024, 1024))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=75)
        b64 = base64.b64encode(buf.getvalue()).decode()

        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 100,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
                    {"type": "text", "text": (
                        "Score this photo as a memory worth keeping from a personal trip. "
                        "Consider: sharpness, exposure, subject interest, faces visible and clear, "
                        "composition, and whether it captures a genuine moment. "
                        "Respond with ONLY a single number from 1 to 10. Nothing else."
                    )},
                ],
            }],
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json=payload,
            )
            text = r.json()["content"][0]["text"].strip()
            return float(text)
    except Exception:
        return 5.0


# ── stage 5: upload to google photos ──────────────────────────────────────────
async def upload_to_google_photos(
    paths: list[Path],
    album_name: str,
    access_token: str,
) -> tuple[Optional[str], Optional[str]]:
    if not access_token:
        return None, None

    headers = {"Authorization": f"Bearer {access_token}", "Content-type": "application/json"}

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            "https://photoslibrary.googleapis.com/v1/albums",
            headers=headers,
            json={"album": {"title": album_name}},
        )
        album = r.json()
        album_id = album.get("id")
        album_url = album.get("productUrl")

        if not album_id:
            return None, None

        media_item_tokens = []
        for path in paths:
            upload_headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-type": "application/octet-stream",
                "X-Goog-Upload-Content-Type": "image/jpeg",
                "X-Goog-Upload-Protocol": "raw",
            }
            r = await client.post(
                "https://photoslibrary.googleapis.com/v1/uploads",
                headers=upload_headers,
                content=path.read_bytes(),
            )
            if r.status_code == 200:
                media_item_tokens.append({
                    "simpleMediaItem": {"uploadToken": r.text, "fileName": path.name}
                })

        if media_item_tokens:
            await client.post(
                "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
                headers=headers,
                json={"albumId": album_id, "newMediaItems": media_item_tokens},
            )

    return album_url, album_id


# ── main pipeline ──────────────────────────────────────────────────────────────
async def run_pipeline(
    job_id: str,
    start_date: date,
    end_date: date,
    album_name: str,
    access_token: str,
    created_album_ids: set = None,
):
    work_dir = WORK_DIR / job_id
    try:
        work_dir.mkdir(parents=True, exist_ok=True)

        # 1. Fetch photo list from Google Photos
        update_job(job_id, status="running", stage="Fetching photo list from Google Photos", progress=5)
        items = await fetch_media_items(access_token, start_date, end_date, job_id)
        total_found = len(items)

        if total_found == 0:
            update_job(job_id, status="error", error=f"No photos found between {start_date} and {end_date}")
            return

        update_job(job_id, total=total_found, stage=f"Found {total_found} photos — downloading", progress=10)

        # 2. Download all photos
        image_paths = await download_all_photos(items, work_dir, access_token, job_id)

        if not image_paths:
            update_job(job_id, status="error", error="Failed to download any photos")
            return

        # 3. Cheap signals pass
        update_job(job_id, stage=f"Analyzing {len(image_paths)} photos", progress=36)
        images = []
        for i, path in enumerate(image_paths):
            if is_screenshot(path):
                continue
            images.append({
                "path": path,
                "blur": blur_score(path),
                "exposure": exposure_score(path),
                "hash": perceptual_hash(path),
                "dt": get_exif_datetime(path),
                "claude_score": None,
            })
            if i % 50 == 0:
                pct = 36 + int((i / len(image_paths)) * 14)
                update_job(job_id, progress=pct, stage=f"Analyzing photos ({i}/{len(image_paths)})")

        images = [img for img in images if img["blur"] > 20 and img["exposure"] > 0.15]

        # 4. Cluster duplicates
        update_job(job_id, stage="Clustering duplicates", progress=52)
        clusters = cluster_duplicates(images)
        candidates = [pick_best_cheap(c) for c in clusters]

        # 5. Claude scoring
        target_count = max(10, int(total_found * TARGET_KEEP_RATIO))
        update_job(job_id, stage=f"AI scoring {len(candidates)} candidates", progress=58)

        scored = []
        for i, img in enumerate(candidates):
            img["claude_score"] = await score_with_claude(img["path"])
            scored.append(img)
            if i % 10 == 0:
                pct = 58 + int((i / len(candidates)) * 28)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Select keepers
        scored.sort(key=lambda x: x["claude_score"], reverse=True)
        keepers = scored[:target_count]
        keeper_paths = [img["path"] for img in keepers]

        # 7. Upload
        update_job(job_id, kept=len(keeper_paths), stage="Creating Google Photos album", progress=88)
        album_url, album_id = await upload_to_google_photos(keeper_paths, album_name, access_token)
        if album_id and created_album_ids is not None:
            created_album_ids.add(album_id)

        update_job(
            job_id,
            status="done",
            stage="Complete",
            progress=100,
            kept=len(keeper_paths),
            album_url=album_url or "no_google_token",
        )

    except Exception as e:
        update_job(job_id, status="error", error=str(e))
    finally:
        if work_dir.exists():
            shutil.rmtree(work_dir)
