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
MANIFEST_DIR = Path("/tmp/photo-curator/manifests")
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
GOOGLE_PHOTOS_PAGE_SIZE = 100


def _parse_manifest(manifest_path):
    """Parse a manifest file into (meta_dict, filenames_set)."""
    meta, filenames = {}, set()
    for line in manifest_path.read_text().splitlines():
        if line.startswith("#"):
            try:
                key, val = line[1:].strip().split("=", 1)
                meta[key.strip()] = val.strip()
            except Exception:
                pass
        elif line.strip():
            filenames.add(line.strip())
    return meta, filenames


def get_album_manifest(album_id: str) -> set:
    """Return set of filenames already uploaded to this album."""
    manifest_path = MANIFEST_DIR / f"{album_id}.txt"
    if not manifest_path.exists():
        return set()
    _, filenames = _parse_manifest(manifest_path)
    return filenames


def get_all_album_manifests() -> list:
    """Return list of all known albums with rich metadata, newest first."""
    albums = []
    for manifest_path in sorted(MANIFEST_DIR.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            album_id = manifest_path.stem
            meta, filenames = _parse_manifest(manifest_path)
            albums.append({
                "album_id": album_id,
                "album_name": meta.get("name", "Unknown Album"),
                "album_url": meta.get("url", ""),
                "photos_count": len(filenames),
                "tranches_count": int(meta.get("tranches", 0)),
                "last_updated": meta.get("last_updated", ""),
                "created": meta.get("created", ""),
                "date_range_start": meta.get("date_range_start", ""),
                "date_range_end": meta.get("date_range_end", ""),
                "scenes": [s for s in meta.get("scenes", "").split(",") if s.strip()],
                "avg_score": float(meta.get("avg_score", 0) or 0),
                "cover_url": meta.get("cover_url", ""),
                "narrative": meta.get("narrative", ""),
                "type": meta.get("type", "curated"),
            })
        except Exception as e:
            pass  # Skip corrupt manifests
    return albums


def create_linked_album(album_url: str, album_name: str, description: str = "") -> dict:
    """Create a manifest entry for a manually linked Google Photos album."""
    import datetime
    import hashlib
    # Generate a stable ID from the URL
    album_id = "linked_" + hashlib.md5(album_url.encode()).hexdigest()[:12]
    manifest_path = MANIFEST_DIR / f"{album_id}.txt"
    if manifest_path.exists():
        return {"error": "Album already linked", "album_id": album_id}

    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(manifest_path, "w") as f:
        f.write(f"# name={album_name}\n")
        f.write(f"# url={album_url}\n")
        f.write(f"# created={now}\n")
        f.write(f"# last_updated={now}\n")
        f.write(f"# type=linked\n")
        if description:
            f.write(f"# narrative={description.replace(chr(10), ' ')}\n")

    _get_logger().info(f"Linked album created: {album_name}", album_id=album_id)
    return {"album_id": album_id, "album_name": album_name}


def update_linked_album(album_id: str, album_name: str = None, description: str = None, cover_url: str = None):
    """Update metadata for a linked album."""
    import datetime
    manifest_path = MANIFEST_DIR / f"{album_id}.txt"
    if not manifest_path.exists():
        return {"error": "Album not found"}

    existing_meta = {}
    for line in manifest_path.read_text().splitlines():
        if line.startswith("#"):
            try:
                key, val = line[1:].strip().split("=", 1)
                existing_meta[key.strip()] = val.strip()
            except Exception:
                pass

    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if album_name:
        existing_meta["name"] = album_name
    if description:
        existing_meta["narrative"] = description.replace("\n", " ")
    if cover_url:
        existing_meta["cover_url"] = cover_url
    existing_meta["last_updated"] = now

    with open(manifest_path, "w") as f:
        for key, val in existing_meta.items():
            f.write(f"# {key}={val}\n")

    return {"ok": True}


def delete_album_manifest(album_id: str) -> dict:
    """Delete an album manifest (unlink or remove curated album record)."""
    manifest_path = MANIFEST_DIR / f"{album_id}.txt"
    if not manifest_path.exists():
        return {"error": "Album not found"}
    manifest_path.unlink()
    _get_logger().info(f"Album manifest deleted", album_id=album_id)
    return {"ok": True}


def update_album_manifest(album_id: str, album_name: str = "", album_url: str = "",
                           filenames: list = None, scenes: list = None, scores: list = None,
                           date_range: tuple = (None, None), cover_url: str = "",
                           narrative: str = ""):
    """Append newly uploaded filenames and update album metadata."""
    import datetime
    filenames = filenames or []
    manifest_path = MANIFEST_DIR / f"{album_id}.txt"
    existing_meta, existing_files = {}, set()
    if manifest_path.exists():
        existing_meta, existing_files = _parse_manifest(manifest_path)

    new_entries = [f for f in filenames if f not in existing_files]

    # Update metadata
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if album_name:
        existing_meta["name"] = album_name
    if album_url:
        existing_meta["url"] = album_url
    existing_meta["last_updated"] = now
    if "created" not in existing_meta:
        existing_meta["created"] = now
    if new_entries:
        existing_meta["tranches"] = str(int(existing_meta.get("tranches", 0)) + 1)

    # Accumulate scenes
    if scenes:
        existing_scenes = set(s.strip() for s in existing_meta.get("scenes", "").split(",") if s.strip())
        existing_scenes.update(s.strip() for s in scenes if s.strip())
        existing_meta["scenes"] = ",".join(sorted(existing_scenes)[:30])

    # Rolling average score
    if scores:
        new_avg = sum(scores) / len(scores)
        old_avg = float(existing_meta.get("avg_score", 0) or 0)
        old_count = max(int(existing_meta.get("tranches", 1)), 1)
        blended = ((old_avg * (old_count - 1)) + new_avg) / old_count
        existing_meta["avg_score"] = f"{blended:.1f}"

    # Date range -- expand to cover all sessions
    if date_range[0]:
        existing_start = existing_meta.get("date_range_start", date_range[0])
        existing_end = existing_meta.get("date_range_end", date_range[1] or date_range[0])
        existing_meta["date_range_start"] = min(existing_start, date_range[0])
        existing_meta["date_range_end"] = max(existing_end, date_range[1] or date_range[0])

    # Cover URL (first session wins)
    if cover_url and not existing_meta.get("cover_url"):
        existing_meta["cover_url"] = cover_url

    # Narrative overwrites with latest full-context version
    if narrative:
        existing_meta["narrative"] = narrative.replace("\n", " ")

    # Rewrite file
    all_files = existing_files | set(new_entries)
    with open(manifest_path, "w") as f:
        for key, val in existing_meta.items():
            f.write(f"# {key}={val}\n")
        for fname in sorted(all_files):
            f.write(fname + "\n")

    _get_logger().info(f"Manifest updated: {len(new_entries)} new, {len(all_files)} total", album_id=album_id)
    return len(new_entries)


def _get_logger():
    """Get the app logger if available, else return a no-op logger."""
    try:
        from main import logger
        return logger
    except Exception:
        class _Noop:
            def info(self, *a, **k): pass
            def warn(self, *a, **k): pass
            def error(self, *a, **k): pass
            def score(self, *a, **k): pass
        return _Noop()


def update_job(job_id: str, **kwargs):
    if job_id in job_store:
        job_store[job_id].update(kwargs)
    if "stage" in kwargs:
        print(f"[JOB] {kwargs['stage']}", flush=True)
        _get_logger().info(f"[JOB] {kwargs['stage']}")
    if kwargs.get("status") == "error" and "error" in kwargs:
        _get_logger().error(f"Job failed: {kwargs['error']}")
    # Push SSE event
    try:
        from state import push_job_event
        event = {"type": "status"}
        event.update({k: v for k, v in kwargs.items()
                      if k in ("stage", "progress", "total", "kept", "status", "error", "album_url", "album_id")})
        push_job_event(job_id, event)
    except Exception:
        pass


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
async def score_with_claude(path: Path, job_id: str = None) -> dict:
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

AUTOMATIC SCORE 1-2 (these should NEVER appear in a family album):
- QR codes, barcodes, tickets, receipts, documents, screenshots
- View out of airplane window with no people visible
- Photos where ALL subjects have backs to camera or heads down/looking away
- Completely dark or completely blown-out photos
- Accidental shots, floor, ceiling, random objects
- Anyone mid-blink, mid-chew, or making an unintentionally bad face

SCORE 3-4 (weak -- exclude unless nothing better exists):
- Airplane cabin shots that are dark, cramped, or unflattering
- People looking away, distracted, not engaged with camera
- Backs of heads even if scenic background
- Empty landscapes with no family members
- Food/drink photos without people in frame
- Hotel rooms, airports, generic travel infrastructure with no people

SCORE 5-6 (acceptable -- include only to fill gaps):
- Decent moments but someone looks slightly off
- OK airplane/travel shots where people look reasonably good
- Scenic shots with at least one family member present

SCORE 7-8 (good -- include these):
- Everyone in frame looks good, genuine moment captured
- Natural smiles, engaged expressions, good lighting
- Activity shots showing family having fun
- Scenic shots where family is prominently featured

SCORE 9-10 (excellent -- must include):
- Perfect family moment, everyone looks their best
- Genuine joy, laughter, connection visible
- Great composition AND people look great
- Will be treasured for years

CRITICAL FLATTERING CHECK:
- If ANY person has their back to camera: flattering=false
- If ANY person is looking down or away: flattering=false  
- If ANY person has eyes closed or bad expression: flattering=false
- If lighting makes anyone look bad: flattering=false
- Only flattering=true if ALL visible people look good"""

        payload = {
            "model": "claude-haiku-4-5-20251001",
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
            if "error" in resp_data or "content" not in resp_data:
                err_msg = resp_data.get("error", {}).get("message", str(resp_data)) if "error" in resp_data else f"No content key in response: {list(resp_data.keys())}"
                _get_logger().error(f"Anthropic API error scoring {path.name}: {err_msg}")
                return {"score": 5.0, "scene": "unknown", "flattering": True, "unflattering_reason": None, "enhance_notes": None}

            text = resp_data["content"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            result = json.loads(text.strip())
            score_val = result.get("score", 5.0)
            scene_val = result.get("scene", "unknown")
            flattering_val = result.get("flattering", True)
            _get_logger().score(path.name, score_val, scene_val, flattering_val)
            # Push score event with base64 thumbnail for live preview
            try:
                from state import push_job_event
                import base64 as _b64
                from PIL import Image as _Img
                import io as _io
                _thumb = _Img.open(path).convert("RGB")
                _thumb.thumbnail((120, 120))
                _buf = _io.BytesIO()
                _thumb.save(_buf, "JPEG", quality=60)
                _thumb_b64 = _b64.b64encode(_buf.getvalue()).decode()
                if job_id:
                    push_job_event(job_id, {
                        "type": "score",
                        "filename": path.name,
                        "score": score_val,
                        "scene": scene_val,
                        "flattering": flattering_val,
                        "unflattering_reason": result.get("unflattering_reason", ""),
                        "thumb": _thumb_b64,
                    })
            except Exception as _push_err:
                _get_logger().error(f"Score event push failed for {path.name}: {_push_err}")
            return result
    except Exception as e:
        _get_logger().error(f'Exception scoring {path.name}: {e}')
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
            "model": "claude-sonnet-4-6",
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
async def generate_album_narrative(album_name: str, all_scenes: list, date_range: tuple, photo_count: int) -> str:
    """Generate a 2-3 sentence human narrative description of the album."""
    if not config.ANTHROPIC_API_KEY:
        return ""
    try:
        scenes_str = ", ".join(list(dict.fromkeys(all_scenes))[:20])  # dedupe, limit 20
        date_str = ""
        if date_range[0] and date_range[1]:
            if date_range[0] == date_range[1]:
                date_str = date_range[0]
            else:
                date_str = f"{date_range[0]} to {date_range[1]}"

        prompt = f"""Write a 2-3 sentence narrative description for a family photo album called "{album_name}".

Context:
- Photo count: {photo_count} photos
- Date range: {date_str or "unknown"}
- Scene types captured: {scenes_str or "family moments"}

Write a warm, vivid description that captures the spirit of the trip. Sound like a human writing a caption, not a robot. Don't use the word "album". Don't start with "This". Be specific about activities and places if the scene tags suggest them. Keep it under 60 words."""

        payload = {
            "model": "claude-sonnet-4-6",
            "max_tokens": 150,
            "messages": [{"role": "user", "content": prompt}],
        }
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": config.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json=payload,
            )
            return r.json()["content"][0]["text"].strip()
    except Exception as e:
        _get_logger().error(f"Narrative generation failed: {e}")
        return ""


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
        _get_logger().warn("No access token -- skipping Google Photos upload")
        return None, None

    BATCH_SIZE = 10  # Upload and create in small batches to avoid timeouts/limits
    headers = {"Authorization": f"Bearer {access_token}", "Content-type": "application/json"}

    async with httpx.AsyncClient(timeout=120) as client:
        # Create or use existing album
        if existing_album_id:
            album_id = existing_album_id
            album_url = existing_album_url
            _get_logger().info(f"Adding to existing album", album_id=album_id)
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
                _get_logger().error(f"Failed to create album: {album}")
                return None, None
            _get_logger().info(f"Created album '{album_name}'", album_id=album_id)

        # Process in batches of BATCH_SIZE
        total = len(paths)
        uploaded = 0
        failed = 0

        for batch_start in range(0, total, BATCH_SIZE):
            batch = paths[batch_start:batch_start + BATCH_SIZE]
            batch_tokens = []

            # 1. Upload bytes for this batch
            for path in batch:
                try:
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
                        batch_tokens.append({
                            "simpleMediaItem": {"uploadToken": r.text, "fileName": path.name}
                        })
                        _get_logger().info(f"Uploaded {path.name} ({batch_start + len(batch_tokens)}/{total})")
                    else:
                        _get_logger().error(f"Upload failed for {path.name}: HTTP {r.status_code}")
                        failed += 1
                except Exception as e:
                    _get_logger().error(f"Upload exception for {path.name}: {e}")
                    failed += 1

            # 2. Immediately batchCreate this batch (tokens are still fresh)
            if batch_tokens:
                try:
                    r = await client.post(
                        "https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate",
                        headers=headers,
                        json={"albumId": album_id, "newMediaItems": batch_tokens},
                    )
                    result = r.json()
                    # Check for partial failures
                    for item_result in result.get("newMediaItemResults", []):
                        status = item_result.get("status", {})
                        if status.get("code", 0) != 0:
                            _get_logger().error(f"batchCreate item failed: {status.get('message')}")
                            failed += 1
                        else:
                            uploaded += 1
                    _get_logger().info(f"Batch {batch_start//BATCH_SIZE + 1}: {len(batch_tokens)} created, running total {uploaded}/{total}")
                except Exception as e:
                    _get_logger().error(f"batchCreate exception for batch {batch_start//BATCH_SIZE + 1}: {e}")
                    failed += len(batch_tokens)

        _get_logger().info(f"Upload complete: {uploaded} succeeded, {failed} failed of {total} total")

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
        update_job(job_id, stage=f"AI scoring {len(candidates)} candidates", progress=50)

        scored = []
        for i, img in enumerate(candidates):
            result = await score_with_claude(img["path"], job_id=job_id)
            img["claude_result"] = result
            img["score"] = result.get("score", 5.0)
            img["flattering"] = result.get("flattering", True)
            img["scene"] = result.get("scene", "")
            img["enhance_notes"] = result.get("enhance_notes")
            img["unflattering_reason"] = result.get("unflattering_reason")
            scored.append(img)
            if i % 10 == 0:
                pct = 50 + int((i / len(candidates)) * 28)
                update_job(job_id, progress=pct, stage=f"AI scoring ({i}/{len(candidates)})")

        # 6. Filter unflattering, select keepers
        flattering = [img for img in scored if img["flattering"] and img.get("score", 5.0) >= config.MIN_SCORE]
        flattering.sort(key=lambda x: x["score"], reverse=True)
        # For small sessions apply stricter ratio; always respect MIN_SCORE cutoff
        # target is % of total input photos, not scored candidates
        target_count = max(10, int(total_found * config.TARGET_KEEP_RATIO))
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
                "tranches": preview_days,
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
                             existing_album_id=None, existing_album_url=None,
                             already_uploaded_ids=None):
    """Upload the approved photo set to Google Photos."""
    try:
        job = job_store[job_id]
        work_dir = Path(job["work_dir"])
        preview = job["preview"]
        album_name = preview["album_name"]

        # Collect approved, non-removed photos
        keeper_paths = []
        keeper_filenames = []
        for day in preview["tranches"]:
            for photo in day["photos"]:
                if not photo.get("removed"):
                    filename = photo["filename"]
                    enhanced = filename.replace(".", "_enhanced.", 1) if photo.get("enhanced") else None
                    path = work_dir / (enhanced if enhanced and (work_dir / enhanced).exists() else filename)
                    if path.exists():
                        keeper_paths.append(path)
                        keeper_filenames.append(filename)

        # Check manifest for already-uploaded filenames if we have an album ID
        skipped_dupes = 0
        if existing_album_id:
            already_uploaded = get_album_manifest(existing_album_id)
            if already_uploaded:
                filtered_paths = []
                filtered_filenames = []
                for path, filename in zip(keeper_paths, keeper_filenames):
                    if filename in already_uploaded:
                        _get_logger().info(f"Skipping {filename} -- already in album")
                        skipped_dupes += 1
                    else:
                        filtered_paths.append(path)
                        filtered_filenames.append(filename)
                keeper_paths = filtered_paths
                keeper_filenames = filtered_filenames
                if skipped_dupes:
                    _get_logger().info(f"Skipped {skipped_dupes} already-uploaded photos")

        stage = f"Adding {len(keeper_paths)} photos to album" if existing_album_id else f"Creating album with {len(keeper_paths)} photos"
        update_job(job_id, status="uploading", stage=stage, progress=92)
        _get_logger().info(f"Starting upload of {len(keeper_paths)} photos", existing_album=bool(existing_album_id), skipped=skipped_dupes)

        album_url, album_id = await upload_to_google_photos(
            keeper_paths, album_name, access_token,
            existing_album_id, existing_album_url
        )

        if album_id and created_album_ids is not None:
            created_album_ids.add(album_id)

        # Collect this session's metadata
        session_scenes = []
        session_scores = []
        session_dates = []
        preview = job.get("preview", {})
        for day in preview.get("days", []):
            for photo in day.get("photos", []):
                if not photo.get("removed"):
                    if photo.get("scene"):
                        session_scenes.append(photo["scene"])
                    if photo.get("score"):
                        session_scores.append(float(photo["score"]))
            if day.get("date") and day["date"] != "unknown":
                session_dates.append(day["date"])

        session_date_range = (
            min(session_dates) if session_dates else None,
            max(session_dates) if session_dates else None
        )
        cover_url = album_url or ""

        # Merge with existing manifest context for full-album narrative
        existing_manifest_meta = {}
        if album_id:
            manifest_path = MANIFEST_DIR / f"{album_id}.txt"
            if manifest_path.exists():
                for line in manifest_path.read_text().splitlines():
                    if line.startswith("#"):
                        try:
                            key, val = line[1:].strip().split("=", 1)
                            existing_manifest_meta[key.strip()] = val.strip()
                        except Exception:
                            pass

        # Build full accumulated scenes for narrative context
        existing_scenes = set(
            s.strip() for s in existing_manifest_meta.get("scenes", "").split(",") if s.strip()
        )
        all_scenes = list(existing_scenes | set(s for s in session_scenes if s.strip()))

        # Build full date range
        existing_start = existing_manifest_meta.get("date_range_start")
        existing_end = existing_manifest_meta.get("date_range_end")
        all_dates = [d for d in [existing_start, existing_end, session_date_range[0], session_date_range[1]] if d]
        full_date_range = (min(all_dates) if all_dates else None, max(all_dates) if all_dates else None)

        # Total photos across all sessions for narrative context
        total_manifest_photos = len(get_album_manifest(album_id)) if album_id else 0
        total_photos_for_narrative = total_manifest_photos + len(keeper_filenames)

        # Generate narrative using FULL album context
        narrative = ""
        if album_id:
            try:
                narrative = await generate_album_narrative(
                    album_name, all_scenes, full_date_range, total_photos_for_narrative
                )
                _get_logger().info(f"Generated narrative: {narrative[:80]}...")
            except Exception as e:
                _get_logger().error(f"Narrative generation error: {e}")

        # Update manifest with this session's data -- writer handles accumulation
        if album_id:
            update_album_manifest(
                album_id, album_name, album_url or "", keeper_filenames,
                scenes=session_scenes, scores=session_scores,
                date_range=session_date_range, cover_url=cover_url,
                narrative=narrative,  # Full album narrative overwrites previous
            )

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
