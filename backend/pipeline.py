import os
import zipfile
import shutil
import json
import base64
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional
import httpx

from state import job_store

# ── constants ──────────────────────────────────────────────────────────────────
WORK_DIR = Path("/tmp/photo-curator/jobs")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
TARGET_KEEP_RATIO = 0.20          # keep ~20% of input
DUPLICATE_HASH_THRESHOLD = 10     # hamming distance for near-duplicate
BURST_WINDOW_SECONDS = 3          # photos within 3s of each other = burst
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_PHOTOS_TOKEN = os.environ.get("GOOGLE_PHOTOS_TOKEN", "")  # set per-session via OAuth


def update_job(job_id: str, **kwargs):
    job_store[job_id].update(kwargs)


# ── stage 1: unzip ─────────────────────────────────────────────────────────────
def unzip_takeout(job_id: str, zip_path: Path) -> Path:
    update_job(job_id, stage="Extracting ZIP", progress=2)
    out_dir = WORK_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)

    return out_dir


def collect_images(extract_dir: Path) -> list[Path]:
    images = []
    for f in extract_dir.rglob("*"):
        if f.suffix.lower() in IMAGE_EXTENSIONS and f.is_file():
            images.append(f)
    return images


# ── stage 2: cheap filters ─────────────────────────────────────────────────────
def is_screenshot(path: Path) -> bool:
    """Detect screenshots by filename pattern."""
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
    """Higher = sharper. Uses Laplacian variance."""
    try:
        import cv2
        import numpy as np
        img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
        if img is None:
            return 0.0
        # Resize for speed
        h, w = img.shape
        if max(h, w) > 1000:
            scale = 1000 / max(h, w)
            img = cv2.resize(img, (int(w * scale), int(h * scale)))
        return float(cv2.Laplacian(img, cv2.CV_64F).var())
    except Exception:
        return 0.0


def exposure_score(path: Path) -> float:
    """Returns 0-1 where 1 = well exposed. Penalizes very dark or blown-out."""
    try:
        from PIL import Image
        import numpy as np
        img = Image.open(path).convert("L").resize((200, 200))
        arr = np.array(img, dtype=float)
        mean = arr.mean()
        # Ideal range: 60-200 out of 255
        if mean < 30:
            return 0.1    # very dark
        if mean > 240:
            return 0.2    # blown out
        # Score peaks around 128
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
    """
    Group images that are near-duplicates (perceptual hash) OR burst shots
    (taken within BURST_WINDOW_SECONDS of each other).
    Returns list of clusters; each cluster = one "moment".
    """
    import imagehash

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

            # Hash similarity check
            if img["hash"] and other["hash"]:
                try:
                    dist = img["hash"] - other["hash"]
                    if dist <= DUPLICATE_HASH_THRESHOLD:
                        cluster.append(other)
                        used.add(j)
                        continue
                except Exception:
                    pass

            # Burst/timestamp check
            if img["dt"] and other["dt"]:
                delta = abs((img["dt"] - other["dt"]).total_seconds())
                if delta <= BURST_WINDOW_SECONDS:
                    cluster.append(other)
                    used.add(j)

        clusters.append(cluster)

    return clusters


def pick_best_cheap(cluster: list[dict]) -> dict:
    """Pick best photo from a cluster using cheap signals only."""
    return max(cluster, key=lambda x: x["blur"] * 0.6 + x["exposure"] * 0.4)


# ── stage 4: claude vision scoring ────────────────────────────────────────────
async def score_with_claude(path: Path) -> float:
    """
    Ask Claude to score a photo 1-10 for overall quality.
    Returns float 0-10, or 5.0 on failure.
    """
    if not ANTHROPIC_API_KEY:
        return 5.0

    try:
        # Resize to max 1024px for cost efficiency
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
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/jpeg", "data": b64},
                        },
                        {
                            "type": "text",
                            "text": (
                                "Score this photo as a memory worth keeping from a personal trip. "
                                "Consider: sharpness, exposure, subject interest, faces visible and clear, "
                                "composition, and whether it captures a genuine moment. "
                                "Respond with ONLY a single number from 1 to 10. Nothing else."
                            ),
                        },
                    ],
                }
            ],
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
            data = r.json()
            text = data["content"][0]["text"].strip()
            return float(text)
    except Exception:
        return 5.0


# ── stage 5: google photos upload ─────────────────────────────────────────────
async def upload_to_google_photos(paths: list[Path], album_name: str) -> Optional[str]:
    """
    Upload curated photos to a new Google Photos album.
    Returns the album URL or None if no token is set.
    """
    if not GOOGLE_PHOTOS_TOKEN:
        return None

    headers = {
        "Authorization": f"Bearer {GOOGLE_PHOTOS_TOKEN}",
        "Content-type": "application/json",
    }

    async with httpx.AsyncClient(timeout=60) as client:
        # Create album
        r = await client.post(
            "https://photoslibrary.googleapis.com/v1/albums",
            headers=headers,
            json={"album": {"title": album_name}},
        )
        album = r.json()
        album_id = album.get("id")
        album_url = album.get("productUrl")

        if not album_id:
            return None

        # Upload each photo
        media_item_tokens = []
        for path in paths:
            with open(path, "rb") as f:
                data = f.read()

            upload_headers = {
                "Authorization": f"Bearer {GOOGLE_PHOTOS_TOKEN}",
                "Content-type": "application/octet-stream",
                "X-Goog-Upload-Content-Type": "image/jpeg",
                "X-Goog-Upload-Protocol": "raw",
            }
            r = await client.post(
                "https://photoslibrary.googleapis.com/v1/uploads",
                headers=upload_headers,
                content=data,
            )
            if r.status_code == 200:
                media_item_tokens.append({
                    "simpleMediaItem": {
                        "uploadToken": r.text,
                        "fileName": path.name,
                    }
                })

        if not media_item_tokens:
            return album_url

        # Batch create media items
        await client.post(
            "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
            headers=headers,
            json={"albumId": album_id, "newMediaItems": media_item_tokens},
        )

    return album_url


# ── main pipeline ──────────────────────────────────────────────────────────────
async def run_pipeline(job_id: str, zip_path: Path, album_name: str):
    try:
        # 1. Extract
        update_job(job_id, status="running", stage="Extracting ZIP", progress=5)
        extract_dir = unzip_takeout(job_id, zip_path)

        # 2. Collect images
        update_job(job_id, stage="Scanning photos", progress=10)
        image_paths = collect_images(extract_dir)
        total = len(image_paths)
        update_job(job_id, total=total)

        if total == 0:
            update_job(job_id, status="error", error="No images found in ZIP")
            return

        # 3. Cheap signals pass
        update_job(job_id, stage=f"Analyzing {total} photos", progress=15)
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
                pct = 15 + int((i / total) * 25)
                update_job(job_id, progress=pct, stage=f"Analyzing photos ({i}/{total})")

        # Filter very blurry / very dark before clustering
        images = [img for img in images if img["blur"] > 20 and img["exposure"] > 0.15]

        # 4. Cluster duplicates
        update_job(job_id, stage="Clustering duplicates", progress=42)
        clusters = cluster_duplicates(images)

        # Pick best from each cluster using cheap signals
        candidates = [pick_best_cheap(c) for c in clusters]

        # 5. Claude scoring on candidates
        target_count = max(10, int(total * TARGET_KEEP_RATIO))
        update_job(job_id, stage=f"Scoring {len(candidates)} candidates with AI", progress=50)

        scored = []
        for i, img in enumerate(candidates):
            score = await score_with_claude(img["path"])
            img["claude_score"] = score
            scored.append(img)
            if i % 10 == 0:
                pct = 50 + int((i / len(candidates)) * 35)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Final selection
        scored.sort(key=lambda x: x["claude_score"], reverse=True)
        keepers = scored[:target_count]
        keeper_paths = [img["path"] for img in keepers]

        update_job(job_id, kept=len(keeper_paths), stage="Uploading to Google Photos", progress=88)

        # 7. Upload
        album_url = await upload_to_google_photos(keeper_paths, album_name)

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
        # Cleanup work dir (keep zip for now)
        work_dir = WORK_DIR / job_id
        if work_dir.exists():
            shutil.rmtree(work_dir)
