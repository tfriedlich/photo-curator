import os
import shutil
import base64
import asyncio
import json
from pathlib import Path
from datetime import datetime, date
from typing import Optional
import httpx

from state import job_store
import config

WORK_DIR = Path("/tmp/photo-curator/jobs")
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
GOOGLE_PHOTOS_PAGE_SIZE = 100

def update_job(job_id: str, **kwargs):
    job_store[job_id].update(kwargs)


# ── Stage 1: Fetch from Google Photos ─────────────────────────────────────────
async def fetch_media_items(access_token, start_date, end_date, job_id):
    headers = {"Authorization": f"Bearer {access_token}"}
    items = []
    next_page_token = None
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body = {
                "pageSize": GOOGLE_PHOTOS_PAGE_SIZE,
                "filters": {
                    "dateFilter": {"ranges": [{"startDate": {"year": start_date.year, "month": start_date.month, "day": start_date.day}, "endDate": {"year": end_date.year, "month": end_date.month, "day": end_date.day}}]},
                    "mediaTypeFilter": {"mediaTypes": ["PHOTO"]},
                    "includeArchivedMedia": True,
                },
            }
            if next_page_token:
                body["pageToken"] = next_page_token
            r = await client.post("https://photoslibrary.googleapis.com/v1/mediaItems:search", headers=headers, json=body)
            data = r.json()
            if "error" in data:
                raise Exception(f"Google Photos API error: {data['error'].get('message', str(data['error']))}")
            batch = data.get("mediaItems", [])
            items.extend(batch)
            update_job(job_id, stage=f"Fetching photo list... ({len(items)} found)", progress=8)
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            await asyncio.sleep(0.1)
    return items


async def download_photo(item, dest_dir, access_token, client):
    try:
        url = item["baseUrl"] + "=d"
        filename = item.get("filename", item["id"] + ".jpg")
        dest = dest_dir / filename
        if dest.exists():
            dest = dest_dir / f"{dest.stem}_{item['id'][:8]}{dest.suffix}"
        r = await client.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code == 200:
            dest.write_bytes(r.content)
            return dest
    except Exception:
        pass
    return None


async def download_all_photos(items, dest_dir, access_token, job_id):
    dest_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(8)
    async def bounded_download(item, idx):
        async with semaphore:
            async with httpx.AsyncClient(timeout=60) as client:
                path = await download_photo(item, dest_dir, access_token, client)
                if idx % 25 == 0:
                    pct = 10 + int((idx / len(items)) * 20)
                    update_job(job_id, stage=f"Downloading photos ({idx}/{len(items)})", progress=pct)
                return path, item
    tasks = [bounded_download(item, i) for i, item in enumerate(items)]
    results = await asyncio.gather(*tasks)
    return [(p, item) for p, item in results if p is not None]


# ── Stage 2: Cheap filters ─────────────────────────────────────────────────────
def is_screenshot(path):
    name = path.name.lower()
    return "screenshot" in name or "screen shot" in name

def get_exif_datetime(path):
    try:
        from PIL import Image
        from PIL.ExifTags import TAGS
        img = Image.open(path)
        exif_data = img._getexif()
        if not exif_data:
            return None
        for tag_id, value in exif_data.items():
            if TAGS.get(tag_id, tag_id) == "DateTimeOriginal":
                return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
    except Exception:
        pass
    return None

def blur_score(path):
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

def exposure_score(path):
    try:
        from PIL import Image
        import numpy as np
        img = Image.open(path).convert("L").resize((200, 200))
        arr = np.array(img, dtype=float)
        mean = arr.mean()
        if mean < 30: return 0.1
        if mean > 240: return 0.2
        return 1.0 - abs(mean - 128) / 128
    except Exception:
        return 0.5

def perceptual_hash(path):
    try:
        import imagehash
        from PIL import Image
        return imagehash.phash(Image.open(path).convert("RGB"))
    except Exception:
        return None


# ── Stage 3: Clustering ────────────────────────────────────────────────────────
def cluster_duplicates(images):
    """
    Two-pass clustering:
    1. Hash similarity (visually near-identical)
    2. Timestamp proximity (same moment, different angle)
    Then merge overlapping clusters.
    """
    n = len(images)
    # Union-Find for efficient merging
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    for i in range(n):
        for j in range(i + 1, n):
            # Hash similarity check
            if images[i]["hash"] and images[j]["hash"]:
                try:
                    if images[i]["hash"] - images[j]["hash"] <= config.DUPLICATE_HASH_THRESHOLD:
                        union(i, j)
                        continue
                except Exception:
                    pass
            # Timestamp proximity check
            if images[i]["dt"] and images[j]["dt"]:
                delta = abs((images[i]["dt"] - images[j]["dt"]).total_seconds())
                if delta <= config.BURST_WINDOW_SECONDS:
                    union(i, j)

    # Group by cluster root
    from collections import defaultdict
    groups = defaultdict(list)
    for i, img in enumerate(images):
        groups[find(i)].append(img)

    return list(groups.values())


def pick_best_from_cluster(cluster: list, max_keep: int = 1) -> list:
    """Keep only the single best photo from each cluster. Best score wins.
    The swap button in the UI lets users cycle through alternatives."""
    sorted_cluster = sorted(cluster, key=lambda x: x.get("score", x.get("cheap_score", 0)), reverse=True)
    return [sorted_cluster[0]]


# ── Stage 4: Claude Vision scoring ────────────────────────────────────────────
async def score_with_claude(path: Path) -> dict:
    """
    Returns structured scoring including flattering check and scene tags.
    """
    if not config.ANTHROPIC_API_KEY:
        print(f"[SCORE] No API key -- returning default 5.0 for {path.name}", flush=True)
        return {"score": 5.0, "scene": "unknown", "flattering": True, "unflattering_reason": None, "enhance_notes": None}

    try:
        from PIL import Image
        import io
        img = Image.open(path).convert("RGB")
        img.thumbnail((1024, 1024))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=75)
        b64 = base64.b64encode(buf.getvalue()).decode()

        prompt = """You are curating a family vacation photo album. Analyze this photo and respond with ONLY valid JSON, no markdown, no explanation.

Return exactly this structure:
{
  "score": <1-10 float>,
  "scene": "<2-4 word scene description>",
  "flattering": <true or false>,
  "unflattering_reason": <null or brief reason>,
  "enhance_notes": <null or brief correction notes>
}

SCORING RULES for a family vacation album:

Score 1-2 (EXCLUDE -- not album-worthy):
- QR codes, barcodes, tickets
- Receipts, credit card statements, financial documents
- Screenshots of apps, maps, messages
- Photos of signs, menus, placards, documents
- Completely empty landscapes with zero people (unless truly breathtaking)
- Accidental shots, floor/ceiling/blur
- Anyone mid-blink, mid-chew, or looking terrible

Score 3-4 (WEAK -- only include if nothing better):
- Technically ok but nobody looks good
- Overly staged or awkward poses
- Landscapes without family members
- Food photos without people
- Random objects (luggage, gear, hotel rooms with no people)
- Backs of heads, people walking away

Score 5-6 (ACCEPTABLE):
- Decent family moments, minor issues
- OK expressions, acceptable composition
- Scenic shots with at least one family member

Score 7-8 (GOOD):
- Everyone looks nice, genuine moment
- Good composition and lighting
- Activity photos where family is clearly having fun

Score 9-10 (EXCELLENT -- must include):
- Perfect family moment, everyone looks great
- Genuine joy, laughter, connection
- Beautiful setting WITH family in it
- Will be treasured for years

FLATTERING check: Are ALL visible people looking good? Eyes open, good expression, not caught awkwardly, lighting kind to faces. If anyone looks bad, flattering=false."""

        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 200,
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
                {"type": "text", "text": prompt},
            ]}],
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": config.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json=payload,
            )
            resp_data = r.json()
            if "error" in resp_data:
                print(f"[SCORE] Anthropic API error: {resp_data['error']}", flush=True)
                return {"score": 5.0, "scene": "unknown", "flattering": True, "unflattering_reason": None, "enhance_notes": None}

            text = resp_data["content"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            result = json.loads(text.strip())
            print(f"[SCORE] {path.name}: {result.get('score')} | {result.get('scene')} | flattering={result.get('flattering')}", flush=True)
            return result
    except Exception as e:
        print(f"[SCORE] Exception scoring {path.name}: {e}", flush=True)
        return {"score": 5.0, "scene": "unknown", "flattering": True, "unflattering_reason": None, "enhance_notes": None}


# ── Stage 5: Trip narrative naming ────────────────────────────────────────────
async def generate_trip_narrative(days_summary: list[dict], album_name: str) -> dict[str, str]:
    """
    Single Claude call to name all days as a coherent travel narrative.
    Returns {date_str: day_name}
    """
    if not config.ANTHROPIC_API_KEY or not days_summary:
        return {}

    try:
        days_text = "\n".join(
            f"Day {i+1} ({d['date']}): {d['photo_count']} photos, locations: {d['locations'] or 'unknown'}, scenes: {d['scenes'] or 'various'}"
            for i, d in enumerate(days_summary)
        )

        prompt = f"""You are naming days of a personal photo album called "{album_name}".

Here are the days:
{days_text}

Give each day a short evocative name (4-8 words) that fits a travel narrative arc.
- Day 1 should feel like arrival/beginning
- Last day should feel like departure/final morning  
- Middle days should capture the main activity or location
- Be specific and evocative, not generic
- Format: "Location · Activity or Mood" e.g. "Arenal · Volcano Hike & Hot Springs" or "Touching Down · San José"
- For home/local events: "Lily's 10th Birthday · Backyard Party" or "Sunday at the Beach · Long Island"

Respond with ONLY valid JSON mapping date to name, no markdown:
{{{", ".join(f'"{d["date"]}": "<name>"' for d in days_summary)}}}"""

        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "messages": [{"role": "user", "content": prompt}],
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": config.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json=payload,
            )
            text = r.json()["content"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            return json.loads(text.strip())
    except Exception:
        return {}


# ── Stage 6: Enhance a single photo ───────────────────────────────────────────
async def enhance_photo(path: Path, enhance_notes: str) -> tuple[Path, str]:
    """
    Apply targeted corrections based on enhance_notes.
    Returns (enhanced_path, description).
    """
    try:
        from PIL import Image, ImageEnhance, ImageFilter
        import numpy as np

        img = Image.open(path).convert("RGB")
        notes = (enhance_notes or "").lower()
        applied = []

        # Exposure
        if any(w in notes for w in ["underexposed", "dark", "too dark", "dim"]):
            enhancer = ImageEnhance.Brightness(img)
            img = enhancer.enhance(1.25)
            applied.append("brightened exposure")
        elif any(w in notes for w in ["overexposed", "bright", "washed out", "blown"]):
            enhancer = ImageEnhance.Brightness(img)
            img = enhancer.enhance(0.82)
            applied.append("reduced exposure")

        # Contrast
        if any(w in notes for w in ["contrast", "flat", "hazy", "foggy"]):
            enhancer = ImageEnhance.Contrast(img)
            img = enhancer.enhance(1.2)
            applied.append("boosted contrast")

        # Sharpness
        if any(w in notes for w in ["soft", "slightly soft", "blur", "unsharp"]):
            enhancer = ImageEnhance.Sharpness(img)
            img = enhancer.enhance(1.4)
            applied.append("sharpened")

        # Color warmth
        if any(w in notes for w in ["cool", "cold", "blue cast", "warm", "color cast"]):
            r, g, b = img.split()
            if "cool" in notes or "cold" in notes or "blue" in notes:
                r = r.point(lambda x: min(255, int(x * 1.08)))
                b = b.point(lambda x: int(x * 0.94))
                applied.append("warmed skin tones")
            else:
                r = r.point(lambda x: int(x * 0.95))
                b = b.point(lambda x: min(255, int(x * 1.06)))
                applied.append("cooled color balance")
            img = Image.merge("RGB", (r, g, b))

        # Saturation boost (almost always helps)
        enhancer = ImageEnhance.Color(img)
        img = enhancer.enhance(1.1)
        if "saturation" not in " ".join(applied):
            applied.append("enhanced colors")

        # Save enhanced version
        enhanced_path = path.parent / f"{path.stem}_enhanced{path.suffix}"
        img.save(enhanced_path, quality=92)

        desc = " · ".join(applied) if applied else "Auto-enhanced"
        return enhanced_path, desc.capitalize()

    except Exception as e:
        return path, "Enhancement failed"


# ── Stage 7: Upload to Google Photos ──────────────────────────────────────────
async def upload_to_google_photos(paths, album_name, access_token,
                                   existing_album_id=None, existing_album_url=None):
    if not access_token:
        return None, None
    headers = {"Authorization": f"Bearer {access_token}", "Content-type": "application/json"}
    async with httpx.AsyncClient(timeout=60) as client:
        # Use existing album or create new one
        if existing_album_id:
            album_id = existing_album_id
            album_url = existing_album_url
        else:
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


# ── Main pipeline ──────────────────────────────────────────────────────────────
async def run_pipeline(job_id, start_date, end_date, album_name, access_token, created_album_ids=None):
    work_dir = WORK_DIR / job_id
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        thumbs_dir = work_dir / "thumbs"
        thumbs_dir.mkdir(exist_ok=True)

        # 1. Fetch
        update_job(job_id, status="running", stage="Fetching photo list from Google Photos", progress=5)
        items = await fetch_media_items(access_token, start_date, end_date, job_id)
        total_found = len(items)
        if total_found == 0:
            update_job(job_id, status="error", error=f"No photos found between {start_date} and {end_date}")
            return
        update_job(job_id, total=total_found, stage=f"Found {total_found} photos — downloading", progress=10)

        # 2. Download
        path_item_pairs = await download_all_photos(items, work_dir, access_token, job_id)
        if not path_item_pairs:
            update_job(job_id, status="error", error="Failed to download any photos")
            return

        # 3. Cheap filters
        update_job(job_id, stage=f"Analyzing {len(path_item_pairs)} photos", progress=32)
        images = []
        for i, (path, item) in enumerate(path_item_pairs):
            if is_screenshot(path):
                continue
            ct = item.get("mediaMetadata", {}).get("creationTime", "")
            day = ct[:10] if ct else str(start_date)
            images.append({
                "path": path,
                "blur": blur_score(path),
                "exposure": exposure_score(path),
                "hash": perceptual_hash(path),
                "dt": get_exif_datetime(path),
                "day": day,
                "item_id": item.get("id", ""),
                "filename": path.name,
                "claude_result": None,
            })
            if i % 50 == 0:
                pct = 32 + int((i / len(path_item_pairs)) * 12)
                update_job(job_id, progress=pct, stage=f"Analyzing photos ({i}/{len(path_item_pairs)})")

        images = [img for img in images if img["blur"] > config.MIN_BLUR_SCORE and img["exposure"] > config.MIN_EXPOSURE_SCORE]

        # 4. Cluster
        update_job(job_id, stage="Clustering duplicates", progress=46)
        clusters = cluster_duplicates(images)

        # Sort each cluster by cheap score, best first
        for cluster in clusters:
            for img in cluster:
                img["cheap_score"] = img["blur"] * 0.6 + img["exposure"] * 0.4
            cluster.sort(key=lambda x: x["cheap_score"], reverse=True)

        # 5. Claude Vision scoring -- score ALL candidates (first in each cluster)
        candidates = [c[0] for c in clusters]
        update_job(job_id, stage=f"AI scoring {len(candidates)} candidates", progress=50)

        scored = []
        for i, img in enumerate(candidates):
            result = await score_with_claude(img["path"])
            img["claude_result"] = result
            img["score"] = result.get("score", 5.0)
            img["flattering"] = result.get("flattering", True)
            img["scene"] = result.get("scene", "")
            img["enhance_notes"] = result.get("enhance_notes")
            img["unflattering_reason"] = result.get("unflattering_reason")
            # Find index in original clusters list
            img["cluster_idx"] = i
            img["cluster_size"] = len(clusters[i])
            scored.append(img)
            if i % 10 == 0:
                pct = 50 + int((i / len(candidates)) * 28)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Filter unflattering, select keepers
        flattering = [img for img in scored if img["flattering"] and img.get("score", 5.0) >= config.MIN_SCORE]
        flattering.sort(key=lambda x: x["score"], reverse=True)
        # For small sessions apply stricter ratio; always respect MIN_SCORE cutoff
        session_ratio = config.TARGET_KEEP_RATIO
        if total_found < 100:
            session_ratio = min(config.TARGET_KEEP_RATIO, 0.30)
        if total_found < 50:
            session_ratio = min(config.TARGET_KEEP_RATIO, 0.25)
        target_count = max(5, int(total_found * session_ratio))
        keepers = flattering[:target_count]

        # Generate thumbnails for preview
        update_job(job_id, stage="Generating preview thumbnails", progress=80)
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

        # 7. Trip narrative naming
        update_job(job_id, stage="Writing trip narrative", progress=84)

        # Group keepers by day
        days_map = {}
        for img in keepers:
            d = img["day"]
            if d not in days_map:
                days_map[d] = []
            days_map[d].append(img)

        # Build days summary for narrative
        days_summary = []
        for d in sorted(days_map.keys()):
            scenes = list(set(img["scene"] for img in days_map[d] if img.get("scene")))
            days_summary.append({
                "date": d,
                "photo_count": len(days_map[d]),
                "scenes": ", ".join(scenes[:5]),
                "locations": None,  # could add GPS reverse geocode here
            })

        day_names = await generate_trip_narrative(days_summary, album_name)

        # 8. Build preview payload
        preview_days = []
        for d in sorted(days_map.keys()):
            day_photos = []
            for img in days_map[d]:
                day_photos.append({
                    "id": img["filename"],
                    "filename": img["filename"],
                    "score": round(img["score"], 1),
                    "scene": img.get("scene", ""),
                    "enhance_notes": img.get("enhance_notes"),
                    "has_alternatives": img["cluster_size"] > 1,
                    "enhanced": False,
                    "removed": False,
                    "cluster_idx": img["cluster_idx"],
                })
            preview_days.append({
                "date": d,
                "day_name": day_names.get(d, d),
                "photos_found": len([img for img in scored if img["day"] == d]),
                "photos": day_photos,
            })

        # Store full preview state in job
        # Keep all scored candidates for swap operations
        all_candidates_serializable = []
        for i, cluster in enumerate(clusters):
            cluster_entries = []
            for img in cluster:
                cluster_entries.append({
                    "filename": img["filename"],
                    "day": img["day"],
                    "score": img.get("score", img.get("cheap_score", 0)),
                    "flattering": img.get("flattering", True),
                    "scene": img.get("scene", ""),
                    "enhance_notes": img.get("enhance_notes"),
                })
            all_candidates_serializable.append(cluster_entries)

        update_job(
            job_id,
            status="preview_ready",
            stage="Preview ready",
            progress=90,
            kept=len(keepers),
            preview={
                "album_name": album_name,
                "total_found": total_found,
                "days": preview_days,
            },
            all_clusters=all_candidates_serializable,
            work_dir=str(work_dir),
        )

    except Exception as e:
        update_job(job_id, status="error", error=str(e))
        work_dir_path = WORK_DIR / job_id
        if work_dir_path.exists():
            shutil.rmtree(work_dir_path)


async def confirm_and_upload(job_id, access_token, created_album_ids=None,
                             existing_album_id=None, existing_album_url=None):
    """Upload the approved photo set to Google Photos."""
    try:
        job = job_store[job_id]
        work_dir = Path(job["work_dir"])
        preview = job["preview"]
        album_name = preview["album_name"]

        stage = "Adding to album" if existing_album_id else "Creating Google Photos album"
        update_job(job_id, status="uploading", stage=stage, progress=92)

        # Collect approved, non-removed photos
        keeper_paths = []
        for day in preview["days"]:
            for photo in day["photos"]:
                if not photo.get("removed"):
                    filename = photo["filename"]
                    enhanced = filename.replace(".", "_enhanced.", 1) if photo.get("enhanced") else None
                    path = work_dir / (enhanced if enhanced and (work_dir / enhanced).exists() else filename)
                    if path.exists():
                        keeper_paths.append(path)

        album_url, album_id = await upload_to_google_photos(
            keeper_paths, album_name, access_token,
            existing_album_id, existing_album_url
        )

        if album_id and created_album_ids is not None:
            created_album_ids.add(album_id)

        update_job(
            job_id,
            status="done",
            stage="Complete",
            progress=100,
            kept=len(keeper_paths),
            album_url=album_url or "no_google_token",
            album_id=album_id or "",
        )

        # Cleanup
        if work_dir.exists():
            shutil.rmtree(work_dir)

    except Exception as e:
        update_job(job_id, status="error", error=str(e))
