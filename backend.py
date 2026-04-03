from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("dutch-audio")
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JUSTWATCH_GRAPHQL = "https://apis.justwatch.com/graphql"
IMAGE_BASE = "https://images.justwatch.com"
COUNTRY = "BE"
LANGUAGE = "nl"
PAGE_SIZE = 100
MAX_OFFSET = 1900  # offset + count must stay < 2000
PAGE_DELAY = 0.5  # seconds between pages per provider
REFRESH_COOLDOWN = 24 * 60 * 60  # 24 hours
DB_PATH = Path(__file__).parent / "titles.db"

PROVIDERS = {
    "nfx": {"name": "Netflix", "color": "#E50914"},
    "dnp": {"name": "Disney+", "color": "#0063e5"},
    "prv": {"name": "Prime Video", "color": "#00A8E1"},
    "atp": {"name": "Apple TV+", "color": "#000000"},
}

# ---------------------------------------------------------------------------
# GraphQL query — based on simple-justwatch-python-api, with audioLanguages
# ---------------------------------------------------------------------------

GQL_QUERY = """
query GetPopularTitles(
  $popularTitlesFilter: TitleFilter,
  $country: Country!,
  $language: Language!,
  $first: Int! = 200,
  $formatPoster: ImageFormat,
  $formatOfferIcon: ImageFormat,
  $profile: PosterProfile,
  $backdropProfile: BackdropProfile,
  $filter: OfferFilter!,
  $offset: Int = 0
) {
  popularTitles(
    country: $country
    filter: $popularTitlesFilter
    first: $first
    sortBy: POPULAR
    sortRandomSeed: 0
    offset: $offset
  ) {
    edges {
      node {
        id
        objectId
        objectType
        content(country: $country, language: $language) {
          title
          originalReleaseYear
          runtime
          shortDescription
          fullPath
          genres { shortName }
          externalIds { imdbId }
          posterUrl(profile: $profile, format: $formatPoster)
          backdrops(profile: $backdropProfile, format: JPG) { backdropUrl }
          scoring {
            imdbScore
            imdbVotes
            tmdbPopularity
            tmdbScore
          }
          ageCertification
        }
        offers(country: $country, platform: WEB, filter: $filter) {
          id
          monetizationType
          standardWebURL
          package {
            packageId
            clearName
            technicalName
            shortName
            icon(profile: S100, format: $formatOfferIcon)
          }
          audioLanguages
          subtitleLanguages
        }
      }
    }
  }
}
"""


def _build_variables(provider: str, offset: int) -> dict:
    return {
        "first": PAGE_SIZE,
        "offset": offset,
        "popularTitlesFilter": {"packages": [provider]},
        "language": LANGUAGE,
        "country": COUNTRY,
        "formatPoster": "JPG",
        "formatOfferIcon": "PNG",
        "profile": "S718",
        "backdropProfile": "S1920",
        "filter": {"bestOnly": True},
    }


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


async def _fetch_page(
    client: httpx.AsyncClient, provider: str, offset: int
) -> list[dict]:
    payload = {
        "operationName": "GetPopularTitles",
        "variables": _build_variables(provider, offset),
        "query": GQL_QUERY,
    }
    resp = await client.post(JUSTWATCH_GRAPHQL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        logger.warning("GraphQL errors for %s offset %d: %s", provider, offset, data["errors"][0].get("message"))
    edges = (data.get("data") or {}).get("popularTitles") or {}
    edges = edges.get("edges", [])
    return [e["node"] for e in edges]


async def _fetch_provider(client: httpx.AsyncClient, provider: str) -> list[dict]:
    all_titles: list[dict] = []
    for offset in range(0, MAX_OFFSET + 1, PAGE_SIZE):
        try:
            page = await _fetch_page(client, provider, offset)
        except Exception:
            logger.exception("Failed fetching %s offset %d", provider, offset)
            break
        all_titles.extend(page)
        logger.info(
            "%s offset=%d → %d titles (total %d)",
            provider,
            offset,
            len(page),
            len(all_titles),
        )
        if len(page) < PAGE_SIZE:
            break
        await asyncio.sleep(PAGE_DELAY)
    return all_titles


def _has_dutch_audio(title: dict) -> bool:
    for offer in title.get("offers") or []:
        if "nl" in (offer.get("audioLanguages") or []):
            return True
    return False


def _poster_url(raw: str | None) -> str | None:
    if not raw:
        return None
    if raw.startswith("http"):
        return raw
    return IMAGE_BASE + raw


def _backdrop_url(content: dict) -> str | None:
    backdrops = content.get("backdrops") or []
    if not backdrops:
        return None
    raw = backdrops[0].get("backdropUrl")
    if not raw:
        return None
    if raw.startswith("http"):
        return raw
    return IMAGE_BASE + raw


TRACKING_PARAMS = {"at", "ct", "itscg", "itsct", "utm_source", "utm_medium",
                   "utm_campaign", "utm_content", "utm_term", "subId1", "subId2",
                   "subId3", "ai"}


def _clean_deeplink(url: str | None) -> str | None:
    """Strip affiliate redirects and tracking params from deeplinks."""
    if not url:
        return None
    parsed = urlparse(url)
    # Unwrap bn5x.net affiliate redirects (Disney+)
    if "bn5x.net" in (parsed.hostname or ""):
        real = parse_qs(parsed.query).get("u", [None])[0]
        if real and real.startswith("https://"):
            parsed = urlparse(real)
        else:
            return url
    # Strip tracking query params
    qs = parse_qs(parsed.query)
    cleaned = {k: v for k, v in qs.items() if k not in TRACKING_PARAMS}
    new_query = urlencode(cleaned, doseq=True) if cleaned else ""
    return urlunparse(parsed._replace(query=new_query))


def _transform_title(title: dict) -> dict:
    content = title.get("content") or {}
    scoring = content.get("scoring") or {}
    external = content.get("externalIds") or {}
    genres = [g["shortName"] for g in (content.get("genres") or [])]

    # Collect platforms with Dutch audio
    platforms: list[dict] = []
    seen_packages: set[str] = set()
    for offer in title.get("offers") or []:
        if "nl" not in (offer.get("audioLanguages") or []):
            continue
        pkg = offer.get("package") or {}
        short = pkg.get("shortName", "")
        if short in seen_packages:
            continue
        seen_packages.add(short)
        icon = pkg.get("icon")
        if icon and not icon.startswith("http"):
            icon = IMAGE_BASE + icon
        platforms.append(
            {
                "name": pkg.get("clearName", short),
                "shortName": short,
                "icon": icon,
                "deeplink": _clean_deeplink(offer.get("standardWebURL")),
            }
        )

    return {
        "id": title.get("objectId"),
        "title": content.get("title"),
        "type": title.get("objectType"),
        "year": content.get("originalReleaseYear"),
        "runtime": content.get("runtime"),
        "synopsis": content.get("shortDescription"),
        "posterUrl": _poster_url(content.get("posterUrl")),
        "backdropUrl": _backdrop_url(content),
        "imdbScore": scoring.get("imdbScore"),
        "imdbId": external.get("imdbId"),
        "genres": genres,
        "ageCertification": content.get("ageCertification"),
        "platforms": platforms,
    }


async def _fetch_all() -> tuple[list[dict], dict[str, int]]:
    """Fetch all providers sequentially, filter for Dutch audio, deduplicate."""
    results: list[list[dict] | Exception] = []
    async with httpx.AsyncClient() as client:
        for provider in PROVIDERS:
            try:
                result = await _fetch_provider(client, provider)
            except Exception as e:
                result = e
            results.append(result)

    counts: dict[str, int] = {}
    merged: dict[str, dict] = {}  # objectId → transformed title

    for provider, result in zip(PROVIDERS, results):
        if isinstance(result, Exception):
            logger.error("Provider %s failed: %s", provider, result)
            counts[provider] = 0
            continue

        dutch_titles = [t for t in result if _has_dutch_audio(t)]
        counts[provider] = len(dutch_titles)
        logger.info(
            "%s: %d total, %d with Dutch audio", provider, len(result), len(dutch_titles)
        )

        for raw_title in dutch_titles:
            oid = raw_title.get("objectId")
            if oid in merged:
                # Merge platforms from this provider into existing entry
                existing_shorts = {
                    p["shortName"] for p in merged[oid]["platforms"]
                }
                new_entry = _transform_title(raw_title)
                for plat in new_entry["platforms"]:
                    if plat["shortName"] not in existing_shorts:
                        merged[oid]["platforms"].append(plat)
            else:
                merged[oid] = _transform_title(raw_title)

    titles = sorted(merged.values(), key=lambda t: t.get("imdbScore") or 0, reverse=True)
    return titles, counts


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------


def _init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            id TEXT PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()


def _load_from_db() -> tuple[list[dict], dict[str, int], float]:
    """Load titles, counts, and updated_at from SQLite. Returns empty data if DB is empty."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT data FROM titles").fetchall()
    titles = [json.loads(r[0]) for r in rows]
    titles.sort(key=lambda t: t.get("imdbScore") or 0, reverse=True)

    counts_row = con.execute("SELECT value FROM meta WHERE key = 'counts'").fetchone()
    counts = json.loads(counts_row[0]) if counts_row else {}

    ts_row = con.execute("SELECT value FROM meta WHERE key = 'updated_at'").fetchone()
    updated_at = float(ts_row[0]) if ts_row else 0.0

    con.close()
    return titles, counts, updated_at


def _save_to_db(titles: list[dict], counts: dict[str, int]) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM titles")
    con.executemany(
        "INSERT INTO titles (id, data) VALUES (?, ?)",
        [(str(t["id"]), json.dumps(t)) for t in titles],
    )
    con.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("counts", json.dumps(counts)),
    )
    con.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        ("updated_at", str(time.time())),
    )
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# In-memory cache (loaded from DB on startup, updated after refresh)
# ---------------------------------------------------------------------------

_cache: dict = {"titles": [], "counts": {}, "updated_at": 0.0}
_refresh_lock = asyncio.Lock()
_refresh_status: dict = {"running": False, "error": None}


async def _refresh_and_persist() -> None:
    """Fetch from JustWatch, update DB and in-memory cache."""
    async with _refresh_lock:
        _refresh_status["running"] = True
        _refresh_status["error"] = None
        try:
            logger.info("Refreshing from JustWatch API...")
            start = time.time()
            titles, counts = await _fetch_all()
            if not titles:
                raise RuntimeError("Fetch returned 0 titles — keeping existing data")
            await asyncio.to_thread(_save_to_db, titles, counts)
            _cache["titles"] = titles
            _cache["counts"] = counts
            _cache["updated_at"] = time.time()
            logger.info(
                "Refresh complete in %.1fs — %d unique titles",
                time.time() - start,
                len(titles),
            )
        except Exception:
            logger.exception("Refresh failed")
            _refresh_status["error"] = "Refresh failed — check server logs"
        finally:
            _refresh_status["running"] = False


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db()
    titles, counts, updated_at = _load_from_db()
    _cache["titles"] = titles
    _cache["counts"] = counts
    _cache["updated_at"] = updated_at
    logger.info("Loaded %d titles from DB (last updated %.0fs ago)",
                len(titles), time.time() - updated_at if updated_at else 0)
    yield


app = FastAPI(title="Dutch Audio Streaming Finder", lifespan=lifespan)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.get("/api/titles")
async def api_titles():
    return _cache["titles"]


@app.post("/api/refresh")
async def api_refresh(request: Request):
    # CSRF protection: require header that triggers CORS preflight
    if not request.headers.get("x-requested-with"):
        return {"status": "forbidden"}

    if _refresh_lock.locked():
        return {"status": "already_running"}

    # 24-hour cooldown
    if _cache["updated_at"] and time.time() - _cache["updated_at"] < REFRESH_COOLDOWN:
        remaining = int(REFRESH_COOLDOWN - (time.time() - _cache["updated_at"]))
        hours = remaining // 3600
        mins = (remaining % 3600) // 60
        return {"status": "cooldown", "retry_after": remaining,
                "message": f"Next refresh available in {hours}h {mins}m"}

    asyncio.create_task(_refresh_and_persist())
    return {"status": "started"}


@app.get("/api/status")
async def api_status():
    age = time.time() - _cache["updated_at"] if _cache["updated_at"] else None
    cooldown_remaining = None
    if _cache["updated_at"]:
        remaining = REFRESH_COOLDOWN - (time.time() - _cache["updated_at"])
        cooldown_remaining = max(0, int(remaining))
    return {
        "cached_titles": len(_cache["titles"]),
        "counts_per_provider": _cache["counts"],
        "cache_age_seconds": round(age) if age else None,
        "refreshing": _refresh_status["running"],
        "refresh_error": _refresh_status["error"],
        "refresh_cooldown_remaining": cooldown_remaining,
    }
