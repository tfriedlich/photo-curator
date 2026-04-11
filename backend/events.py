"""
Event detection: analyzes photo metadata to find notable days/trips.
No image downloading -- metadata only.
"""
import asyncio
from datetime import date, datetime, timedelta
from collections import defaultdict
from math import radians, sin, cos, sqrt, atan2
from typing import Optional
import httpx

import config


def haversine_km(lat1, lng1, lat2, lng2) -> float:
    """Distance between two GPS points in km."""
    R = 6371
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def is_near_home(lat, lng) -> bool:
    return haversine_km(lat, lng, config.HOME_LAT, config.HOME_LNG) <= config.HOME_RADIUS_KM


async def reverse_geocode(lat: float, lng: float, client: httpx.AsyncClient) -> dict:
    """Returns address dict from OSM Nominatim."""
    try:
        r = await client.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lng, "format": "json", "zoom": 10},
            headers={"User-Agent": "PhotoCurator/1.0"},
            timeout=5,
        )
        data = r.json()
        place_type = data.get("type", "") or data.get("class", "")
        address = data.get("address", {})
        return {
            "is_airport": place_type in ("aerodrome", "aeroway") or
                          "airport" in data.get("display_name", "").lower(),
            "city": address.get("city") or address.get("town") or address.get("village") or address.get("suburb"),
            "state": address.get("state"),
            "country": address.get("country"),
            "country_code": address.get("country_code", "").upper(),
        }
    except Exception:
        return {}


def suggest_album_name(locations: list[dict], start: date, end: date, is_home: bool) -> str:
    """
    Build a smart album name from a list of location dicts.
    Filters airports. Rolls up to country/region when multiple cities.
    """
    month_year = start.strftime("%B %Y") if start.month == end.month else \
                 f"{start.strftime('%b')}–{end.strftime('%b %Y')}"

    # Filter airports
    locs = [l for l in locations if not l.get("is_airport") and l.get("city") or l.get("country")]

    if not locs:
        if is_home:
            return f"Home · {start.strftime('%B %d, %Y')}" if start == end else f"Home · {month_year}"
        return f"Best Of {month_year}"

    countries = list(dict.fromkeys(l["country"] for l in locs if l.get("country")))
    cities    = list(dict.fromkeys(l["city"] for l in locs if l.get("city")))

    if is_home and len(countries) <= 1 and len(cities) <= 2:
        # Local event e.g. birthday party
        city = cities[0] if cities else countries[0]
        return f"{city} · {start.strftime('%B %d, %Y')}" if start == end else f"{city} · {month_year}"

    if len(countries) == 1:
        country = countries[0]
        if len(cities) <= 2:
            return f"{' & '.join(cities)} · {month_year}"
        else:
            # Many cities in one country → use country name
            return f"{country} · {month_year}"

    # Multiple countries → try to find a region name
    REGIONS = {
        frozenset(["DE", "FR", "IT", "ES", "PT", "NL", "BE", "AT", "CH", "PL", "CZ", "HU", "GR", "SE", "NO", "DK", "FI"]): "Europe",
        frozenset(["TH", "VN", "ID", "MY", "SG", "PH", "KH", "MM", "LA"]): "Southeast Asia",
        frozenset(["JP", "KR", "CN", "TW", "HK"]): "East Asia",
        frozenset(["MX", "GT", "BZ", "HN", "SV", "NI", "CR", "PA"]): "Central America",
        frozenset(["CO", "PE", "BR", "AR", "CL", "EC", "BO", "UY", "PY", "VE"]): "South America",
        frozenset(["KE", "TZ", "UG", "RW", "ET", "ZA", "BW", "NA", "ZW", "MZ"]): "Africa",
        frozenset(["MA", "TN", "EG", "JO", "IL", "LB", "TR", "AE", "QA", "SA"]): "Middle East & North Africa",
        frozenset(["IN", "NP", "LK", "BD", "PK"]): "South Asia",
    }
    codes = set(l["country_code"] for l in locs if l.get("country_code"))
    for region_codes, region_name in REGIONS.items():
        if codes.issubset(region_codes):
            return f"{region_name} · {month_year}"

    # Fallback: first two countries
    return f"{' & '.join(countries[:2])} · {month_year}"


async def fetch_photo_histogram(
    access_token: str,
    days: int,
) -> dict[str, list]:
    """
    Fetch metadata for all photos in the last `days` days.
    Returns {date_str: [{"lat": ..., "lng": ...}, ...]}
    """
    end_date   = date.today()
    start_date = end_date - timedelta(days=days)

    headers = {"Authorization": f"Bearer {access_token}"}
    day_map: dict[str, list] = defaultdict(list)
    next_page_token = None

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body = {
                "pageSize": 100,
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
                break

            for item in data.get("mediaItems", []):
                ct = item.get("mediaMetadata", {}).get("creationTime", "")
                day = ct[:10] if ct else None
                if not day:
                    continue
                meta = item.get("mediaMetadata", {})
                lat = meta.get("photo", {}).get("latitude")
                lng = meta.get("photo", {}).get("longitude")
                day_map[day].append({"lat": lat, "lng": lng})

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            await asyncio.sleep(0.05)

    return dict(day_map)


def detect_events(day_map: dict[str, list]) -> list[dict]:
    """
    Find notable clusters of days using baseline multiplier.
    Returns list of {start, end, photo_count, lat_lngs}
    """
    if not day_map:
        return []

    # Build sorted day counts
    all_days = sorted(day_map.keys())
    counts = {d: len(day_map[d]) for d in all_days}

    # Baseline = median daily count
    vals = sorted(counts.values())
    median = vals[len(vals)//2] if vals else 1
    baseline = max(median, 2)  # floor of 2 to avoid div-by-zero on very sparse libraries
    threshold = baseline * config.BASELINE_MULTIPLIER

    # Flag notable days
    notable = {d for d, c in counts.items() if c >= threshold and c >= config.MIN_PHOTOS_FOR_EVENT}

    if not notable:
        return []

    # Merge adjacent/nearby notable days (gap of 1 normal day allowed)
    sorted_notable = sorted(notable)
    clusters = []
    current = [sorted_notable[0]]

    for d in sorted_notable[1:]:
        prev = date.fromisoformat(current[-1])
        curr = date.fromisoformat(d)
        if (curr - prev).days <= 2:  # allow 1 quiet day gap inside event
            current.append(d)
        else:
            clusters.append(current)
            current = [d]
    clusters.append(current)

    events = []
    for cluster in clusters:
        start = date.fromisoformat(cluster[0])
        end   = date.fromisoformat(cluster[-1])
        # Collect all lat/lngs for the cluster
        lat_lngs = []
        total = 0
        for d in cluster:
            items = day_map.get(d, [])
            total += len(items)
            for item in items:
                if item.get("lat") and item.get("lng"):
                    lat_lngs.append((item["lat"], item["lng"]))

        events.append({
            "start": start.isoformat(),
            "end":   end.isoformat(),
            "photo_count": total,
            "lat_lngs": lat_lngs,
        })

    # Sort by recency
    events.sort(key=lambda e: e["start"], reverse=True)
    return events


async def enrich_events(events: list[dict]) -> list[dict]:
    """Add location names and suggested album names to events."""
    async with httpx.AsyncClient(timeout=10) as client:
        for event in events:
            lat_lngs = event.pop("lat_lngs", [])
            start = date.fromisoformat(event["start"])
            end   = date.fromisoformat(event["end"])

            if not lat_lngs:
                event["location"] = None
                event["album_name"] = suggest_album_name([], start, end, False)
                continue

            # Sample up to 20 points spread across the event
            step = max(1, len(lat_lngs) // 20)
            sampled = lat_lngs[::step][:20]

            # Geocode each sampled point
            geocoded = []
            seen = set()
            for lat, lng in sampled:
                if config.AIRPORT_FILTER:
                    pass  # checked inside reverse_geocode
                geo = await reverse_geocode(lat, lng, client)
                if not geo or geo.get("is_airport"):
                    continue
                key = (geo.get("city"), geo.get("country"))
                if key not in seen:
                    seen.add(key)
                    geocoded.append(geo)
                await asyncio.sleep(0.1)  # Nominatim rate limit

            # Check if primarily home
            home_count = sum(1 for lat, lng in sampled if is_near_home(lat, lng))
            is_home = home_count > len(sampled) * 0.6

            # Build display location string
            countries = list(dict.fromkeys(g["country"] for g in geocoded if g.get("country")))
            cities    = list(dict.fromkeys(g["city"] for g in geocoded if g.get("city")))
            if cities and len(cities) <= 3:
                event["location"] = ", ".join(cities[:3])
            elif countries:
                event["location"] = ", ".join(countries[:2])
            else:
                event["location"] = None

            event["album_name"] = suggest_album_name(geocoded, start, end, is_home)
            await asyncio.sleep(0.1)

    return events


async def get_events(access_token: str, days: int = None) -> list[dict]:
    """Full pipeline: fetch histogram → detect → enrich."""
    if days is None:
        days = config.LOOKBACK_DAYS
    days = min(days, config.MAX_LOOKBACK_DAYS)

    day_map = await fetch_photo_histogram(access_token, days)
    events  = detect_events(day_map)
    events  = await enrich_events(events)
    return events
