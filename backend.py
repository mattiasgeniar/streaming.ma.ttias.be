from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx
from dotenv import load_dotenv
from PIL import Image
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv(Path(__file__).parent / ".env")

STATIC_DIR = Path(__file__).parent / "static"

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
REFRESH_COOLDOWN = 23 * 60 * 60  # 23 hours (allows daily cron with some drift)
DB_PATH = Path(__file__).parent / "titles.db"
REFRESH_SECRET = os.getenv("REFRESH_SECRET", "")

IMAGE_DIR = STATIC_DIR / "images"
POSTER_DIR = IMAGE_DIR / "posters"
POSTER_THUMB_DIR = POSTER_DIR / "thumbs"
BACKDROP_DIR = IMAGE_DIR / "backdrops"
ICON_DIR = IMAGE_DIR / "icons"
DOWNLOAD_CONCURRENCY = 8

PROVIDER_ICON_FILENAMES = {
    "nfx": "netflix.png",
    "dnp": "disneyplus.png",
    "prv": "amazonprimevideo.png",
    "atp": "appletvplus.png",
}

PROVIDERS = {
    "nfx": {"name": "Netflix", "color": "#E50914"},
    "dnp": {"name": "Disney+", "color": "#0063e5"},
    "prv": {"name": "Prime Video", "color": "#00A8E1"},
    "atp": {"name": "Apple TV+", "color": "#000000"},
}

# Streaming Availability API
SA_BASE = "https://streaming-availability.p.rapidapi.com"
SA_CATALOGS = "netflix,disney,prime.subscription,apple"
SA_MAX_REQUESTS_PER_REFRESH = 80  # leave headroom in 100/day free tier
# Map SA service IDs to our provider short names
SA_SERVICE_MAP = {
    "netflix": "nfx",
    "disney": "dnp",
    "prime": "prv",
    "apple": "atp",
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
# Shared helpers
# ---------------------------------------------------------------------------

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


def _resolve_image_url(raw: str | None) -> str | None:
    """Prepend IMAGE_BASE to relative JustWatch image URLs."""
    if not raw:
        return None
    if raw.startswith("http"):
        return raw
    return IMAGE_BASE + raw


# ---------------------------------------------------------------------------
# Image download pipeline
# ---------------------------------------------------------------------------


def _slugify(text: str) -> str:
    """Unicode normalize -> ASCII -> lowercase -> replace non-alnum with hyphens -> truncate."""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return text[:80]


def _image_filename(title: dict) -> str:
    """Return SEO-friendly filename like 'the-matrix-tm12345'."""
    slug = _slugify(title.get("title", "") or "untitled")
    return f"{slug}-{title['id']}" if slug else str(title["id"])


THUMB_WIDTH = 332  # ~2x for 150px grid cards on retina displays


def _resize_to_thumb(src: Path, dest: Path) -> bool:
    """Resize poster to thumbnail width, preserving aspect ratio."""
    try:
        with Image.open(src) as img:
            if img.width <= THUMB_WIDTH:
                # Already small enough — just copy
                import shutil
                shutil.copy2(src, dest)
                return True
            ratio = THUMB_WIDTH / img.width
            new_h = int(img.height * ratio)
            resized = img.resize((THUMB_WIDTH, new_h), Image.LANCZOS)
            resized.save(dest, "JPEG", quality=85)
            return True
    except Exception:
        logger.warning("Failed to resize %s", src)
        return False


async def _download_image(
    client: httpx.AsyncClient, url: str, dest: Path, sem: asyncio.Semaphore,
) -> bool:
    """Download image to dest with atomic write. Skips if file already exists."""
    if dest.exists():
        return True
    async with sem:
        try:
            resp = await client.get(url, timeout=15, follow_redirects=True)
            resp.raise_for_status()
            tmp = dest.with_suffix(f".tmp{os.getpid()}")
            tmp.write_bytes(resp.content)
            tmp.rename(dest)
            return True
        except Exception:
            logger.warning("Failed to download image: %s", url)
            return False


async def _ensure_title_images(
    client: httpx.AsyncClient, title: dict, sem: asyncio.Semaphore,
) -> None:
    """Download poster and backdrop, generate thumb by resizing poster locally."""
    fname = _image_filename(title)
    poster_url = title.get("posterUrl")
    backdrop_url = title.get("backdropUrl")

    poster_dest = POSTER_DIR / f"{fname}.jpg"
    thumb_dest = POSTER_THUMB_DIR / f"{fname}.jpg"
    backdrop_dest = BACKDROP_DIR / f"{fname}.jpg"

    coros: list = []
    if poster_url and poster_url.startswith("http"):
        coros.append(_download_image(client, poster_url, poster_dest, sem))
    if backdrop_url and backdrop_url.startswith("http"):
        coros.append(_download_image(client, backdrop_url, backdrop_dest, sem))

    if coros:
        await asyncio.gather(*coros, return_exceptions=True)

    # Generate thumbnail from downloaded poster
    if poster_dest.exists() and not thumb_dest.exists():
        await asyncio.to_thread(_resize_to_thumb, poster_dest, thumb_dest)

    # Rewrite URLs to local paths — keep original http URL on failure (retried on next startup)
    if poster_dest.exists():
        title["posterUrl"] = f"/static/images/posters/{fname}.jpg"
    if thumb_dest.exists():
        title["posterThumbUrl"] = f"/static/images/posters/thumbs/{fname}.jpg"
    if backdrop_dest.exists():
        title["backdropUrl"] = f"/static/images/backdrops/{fname}.jpg"

    for p in title.get("platforms", []):
        short = p.get("shortName", "")
        if short in PROVIDER_ICON_FILENAMES:
            icon_path = ICON_DIR / PROVIDER_ICON_FILENAMES[short]
            if icon_path.exists():
                p["icon"] = f"/static/images/icons/{PROVIDER_ICON_FILENAMES[short]}"


async def _download_all_images(titles: list[dict]) -> None:
    """Download all images for titles and rewrite URLs to local paths."""
    for d in (POSTER_DIR, POSTER_THUMB_DIR, BACKDROP_DIR, ICON_DIR):
        d.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    async with httpx.AsyncClient() as client:
        # Pre-download provider icons
        icon_coros = []
        for short, filename in PROVIDER_ICON_FILENAMES.items():
            url = SA_PROVIDER_ICONS.get(short)
            if url:
                icon_coros.append(_download_image(client, url, ICON_DIR / filename, sem))
        if icon_coros:
            await asyncio.gather(*icon_coros, return_exceptions=True)

        # Download title images concurrently
        coros = [_ensure_title_images(client, t, sem) for t in titles]
        await asyncio.gather(*coros, return_exceptions=True)

    logger.info("Image download complete for %d titles", len(titles))


# ---------------------------------------------------------------------------
# JustWatch fetching
# ---------------------------------------------------------------------------


async def _jw_fetch_page(
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
    popular = (data.get("data") or {}).get("popularTitles") or {}
    edges = popular.get("edges", [])
    return [e["node"] for e in edges]


async def _jw_fetch_provider(client: httpx.AsyncClient, provider: str) -> list[dict]:
    all_titles: list[dict] = []
    for offset in range(0, MAX_OFFSET + 1, PAGE_SIZE):
        try:
            page = await _jw_fetch_page(client, provider, offset)
        except Exception:
            logger.exception("Failed fetching %s offset %d", provider, offset)
            break
        all_titles.extend(page)
        logger.info(
            "JW %s offset=%d → %d titles (total %d)",
            provider, offset, len(page), len(all_titles),
        )
        if len(page) < PAGE_SIZE:
            break
        await asyncio.sleep(PAGE_DELAY)
    return all_titles


def _jw_has_dutch_audio(title: dict) -> bool:
    for offer in title.get("offers") or []:
        if "nl" in (offer.get("audioLanguages") or []):
            return True
    return False


def _jw_transform_title(title: dict) -> dict:
    content = title.get("content") or {}
    scoring = content.get("scoring") or {}
    external = content.get("externalIds") or {}
    genres = [g["shortName"] for g in (content.get("genres") or [])]

    # Backdrop
    backdrops = content.get("backdrops") or []
    backdrop_url = _resolve_image_url(backdrops[0].get("backdropUrl")) if backdrops else None

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
        platforms.append(
            {
                "name": pkg.get("clearName", short),
                "shortName": short,
                "icon": _resolve_image_url(pkg.get("icon")),
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
        "posterUrl": _resolve_image_url(content.get("posterUrl")),
        "backdropUrl": backdrop_url,
        "imdbScore": scoring.get("imdbScore"),
        "imdbId": external.get("imdbId"),
        "genres": genres,
        "ageCertification": content.get("ageCertification"),
        "platforms": platforms,
        "source": "justwatch",
    }


async def _jw_fetch_all() -> tuple[dict[str, dict], dict[str, int]]:
    """Fetch all providers from JustWatch. Returns merged dict keyed by ID and counts."""
    results: list[list[dict] | Exception] = []
    async with httpx.AsyncClient() as client:
        for provider in PROVIDERS:
            try:
                result = await _jw_fetch_provider(client, provider)
            except Exception as e:
                result = e
            results.append(result)

    counts: dict[str, int] = {}
    merged: dict[str, dict] = {}

    for provider, result in zip(PROVIDERS, results):
        if isinstance(result, Exception):
            logger.error("JW provider %s failed: %s", provider, result)
            counts[provider] = 0
            continue

        dutch_titles = [t for t in result if _jw_has_dutch_audio(t)]
        counts[provider] = len(dutch_titles)
        logger.info(
            "JW %s: %d total, %d with Dutch audio", provider, len(result), len(dutch_titles)
        )

        for raw_title in dutch_titles:
            oid = raw_title.get("objectId")
            if not oid:
                continue
            if oid in merged:
                existing_shorts = {p["shortName"] for p in merged[oid]["platforms"]}
                new_entry = _jw_transform_title(raw_title)
                for plat in new_entry["platforms"]:
                    if plat["shortName"] not in existing_shorts:
                        merged[oid]["platforms"].append(plat)
            else:
                merged[oid] = _jw_transform_title(raw_title)

    return merged, counts


# ---------------------------------------------------------------------------
# Streaming Availability API fetching
# ---------------------------------------------------------------------------

SA_PROVIDER_ICONS = {
    "nfx": "https://images.justwatch.com/icon/207360008/s100/netflix.png",
    "dnp": "https://images.justwatch.com/icon/313118777/s100/disneyplus.png",
    "prv": "https://images.justwatch.com/icon/322992749/s100/amazonprimevideo.png",
    "atp": "https://images.justwatch.com/icon/338367329/s100/appletvplus.png",
}

SA_PROVIDER_NAMES = {
    "nfx": "Netflix",
    "dnp": "Disney+",
    "prv": "Amazon Prime Video",
    "atp": "Apple TV",
}


def _sa_has_dutch_audio(show: dict) -> bool:
    """Check if any BE streaming option has Dutch audio."""
    for opt in show.get("streamingOptions", {}).get("be", []):
        svc = opt.get("service", {}).get("id", "")
        if svc not in SA_SERVICE_MAP:
            continue
        for audio in opt.get("audios", []):
            if audio.get("language") in ("nld", "dut"):
                return True
    return False


def _sa_transform_show(show: dict) -> dict:
    """Transform a Streaming Availability show into our common format."""
    # Collect platforms with Dutch audio
    platforms: list[dict] = []
    seen: set[str] = set()
    for opt in show.get("streamingOptions", {}).get("be", []):
        svc_id = opt.get("service", {}).get("id", "")
        short = SA_SERVICE_MAP.get(svc_id)
        if not short or short in seen:
            continue
        audios = [a.get("language") for a in opt.get("audios", [])]
        if "nld" not in audios and "dut" not in audios:
            continue
        seen.add(short)
        platforms.append({
            "name": SA_PROVIDER_NAMES.get(short, svc_id),
            "shortName": short,
            "icon": SA_PROVIDER_ICONS.get(short),
            "deeplink": _clean_deeplink(opt.get("link")),
        })

    # Image URLs
    poster = None
    backdrop = None
    posters = show.get("imageSet", {})
    if posters.get("verticalPoster", {}).get("w720"):
        poster = posters["verticalPoster"]["w720"]
    backdrop_data = posters.get("horizontalBackdrop", {})
    if backdrop_data.get("w1080"):
        backdrop = backdrop_data["w1080"]

    # IMDb score — SA returns as 0-100 int
    imdb_id = show.get("imdbId")
    imdb_score_raw = show.get("rating")
    imdb_score = imdb_score_raw / 10 if imdb_score_raw else None

    show_type = show.get("showType", "").upper()
    if show_type == "SERIES":
        show_type = "SHOW"

    return {
        "id": f"sa-{show.get('id', '')}",
        "title": show.get("title"),
        "type": show_type or None,
        "year": show.get("releaseYear") or show.get("firstAirYear"),
        "runtime": show.get("runtime"),
        "synopsis": show.get("overview"),
        "posterUrl": poster,
        "backdropUrl": backdrop,
        "imdbScore": imdb_score,
        "imdbId": imdb_id,
        "genres": [g.get("id", "") for g in show.get("genres", [])],
        "ageCertification": None,
        "platforms": platforms,
        "source": "streaming-availability",
    }


async def _sa_fetch_page(client: httpx.AsyncClient, cursor: str | None = None) -> tuple[list[dict], str | None]:
    """Fetch one page from Streaming Availability API. Returns (shows, next_cursor)."""
    api_key = os.getenv("RAPIDAPI_KEY", "")
    if not api_key:
        return [], None

    params: dict = {
        "country": "be",
        "catalogs": SA_CATALOGS,
        "order_by": "popularity_1year",
        "output_language": "en",
    }
    if cursor:
        params["cursor"] = cursor

    resp = await client.get(
        f"{SA_BASE}/shows/search/filters",
        params=params,
        headers={
            "x-rapidapi-host": "streaming-availability.p.rapidapi.com",
            "x-rapidapi-key": api_key,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("shows", []), data.get("nextCursor")


async def _sa_fetch_all(resume_cursor: str | None = None) -> tuple[dict[str, dict], dict[str, int], str | None]:
    """Fetch from Streaming Availability API with budget limit.
    Returns (merged_dict, counts, last_cursor_for_resume).
    """
    api_key = os.getenv("RAPIDAPI_KEY", "")
    if not api_key:
        logger.info("SA: No RAPIDAPI_KEY configured, skipping")
        return {}, {}, None

    merged: dict[str, dict] = {}
    counts: dict[str, int] = {"nfx": 0, "dnp": 0, "prv": 0, "atp": 0}
    cursor = resume_cursor
    requests_used = 0

    async with httpx.AsyncClient() as client:
        while requests_used < SA_MAX_REQUESTS_PER_REFRESH:
            try:
                shows, next_cursor = await _sa_fetch_page(client, cursor)
                requests_used += 1
            except Exception:
                logger.exception("SA: Failed at request %d, cursor=%s", requests_used, cursor)
                break

            logger.info("SA: page %d → %d shows", requests_used, len(shows))

            for show in shows:
                if not _sa_has_dutch_audio(show):
                    continue
                transformed = _sa_transform_show(show)
                if not transformed["platforms"]:
                    continue
                sid = transformed["id"]
                if sid not in merged:
                    merged[sid] = transformed
                    for p in transformed["platforms"]:
                        sn = p["shortName"]
                        if sn in counts:
                            counts[sn] += 1

            cursor = next_cursor
            if not cursor:
                logger.info("SA: Reached end of catalog after %d requests", requests_used)
                break
            await asyncio.sleep(0.3)

    logger.info("SA: %d requests used, %d Dutch audio titles found", requests_used, len(merged))
    # Return None cursor if we finished the catalog, otherwise return cursor to resume later
    return merged, counts, cursor if cursor else None


# ---------------------------------------------------------------------------
# Merged refresh
# ---------------------------------------------------------------------------


async def _fetch_all() -> tuple[list[dict], dict[str, int]]:
    """Fetch from both JustWatch and Streaming Availability, merge results.

    JustWatch: full rescan every time (handles removals naturally).
    SA: paginate incrementally across refreshes. When the cursor reaches the end
    (full cycle complete), stale SA-only titles are dropped on the next cycle.
    """
    # Fetch from JustWatch (primary source — has rich metadata)
    jw_merged, jw_counts = await _jw_fetch_all()
    logger.info("JW total: %d Dutch audio titles", len(jw_merged))

    # Load previously accumulated SA titles from DB
    existing_sa = _load_sa_titles_from_db()
    logger.info("SA existing: %d titles from previous scans", len(existing_sa))

    # Fetch new SA pages (incremental)
    sa_cursor = _get_sa_cursor()
    sa_new, sa_counts, new_cursor = await _sa_fetch_all(resume_cursor=sa_cursor)
    logger.info("SA new: %d Dutch audio titles from this scan", len(sa_new))

    # Merge new SA titles into existing SA collection
    existing_sa.update(sa_new)

    if new_cursor is None and sa_cursor is not None:
        # Full cycle completed — we've seen the entire SA catalog.
        # On the NEXT refresh, we start fresh (existing_sa will be rebuilt from scratch).
        # For now, keep everything we found in this completed cycle.
        logger.info("SA: Full catalog scan completed! %d total SA titles", len(existing_sa))

    _set_sa_cursor(new_cursor)
    _save_sa_titles_to_db(existing_sa)

    # Merge SA into JW: match by imdbId where possible
    jw_by_imdb: dict[str, str] = {}
    for oid, title in jw_merged.items():
        imdb = title.get("imdbId")
        if imdb:
            jw_by_imdb[imdb] = oid

    added_from_sa = 0
    merged_platforms_from_sa = 0
    for sa_id, sa_title in existing_sa.items():
        imdb = sa_title.get("imdbId")
        jw_oid = jw_by_imdb.get(imdb) if imdb else None

        if jw_oid and jw_oid in jw_merged:
            # Title exists in JustWatch — merge any missing platforms
            existing_shorts = {p["shortName"] for p in jw_merged[jw_oid]["platforms"]}
            for plat in sa_title["platforms"]:
                if plat["shortName"] not in existing_shorts:
                    jw_merged[jw_oid]["platforms"].append(plat)
                    merged_platforms_from_sa += 1
        else:
            # New title only in SA — add it
            jw_merged[sa_id] = sa_title
            added_from_sa += 1

    logger.info("Merge: %d SA-only titles, %d platforms added to existing titles",
                added_from_sa, merged_platforms_from_sa)

    # Combine counts
    counts = {}
    for p in PROVIDERS:
        counts[p] = jw_counts.get(p, 0) + sa_counts.get(p, 0)

    titles = sorted(jw_merged.values(), key=lambda t: t.get("imdbScore") or 0, reverse=True)
    return titles, counts


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------


def _init_db() -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sa_titles (
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


def _load_from_db() -> tuple[list[dict], dict[str, int], float]:
    """Load titles, counts, and updated_at from SQLite. Returns empty data if DB is empty."""
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute("SELECT data FROM titles").fetchall()
        titles = [json.loads(r[0]) for r in rows]
        titles.sort(key=lambda t: t.get("imdbScore") or 0, reverse=True)

        counts_row = con.execute("SELECT value FROM meta WHERE key = 'counts'").fetchone()
        counts = json.loads(counts_row[0]) if counts_row else {}

        ts_row = con.execute("SELECT value FROM meta WHERE key = 'updated_at'").fetchone()
        updated_at = float(ts_row[0]) if ts_row else 0.0

    return titles, counts, updated_at


def _save_to_db(titles: list[dict], counts: dict[str, int]) -> None:
    with sqlite3.connect(DB_PATH) as con:
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


def _get_sa_cursor() -> str | None:
    """Get the saved SA pagination cursor for incremental fetching."""
    try:
        with sqlite3.connect(DB_PATH) as con:
            row = con.execute("SELECT value FROM meta WHERE key = 'sa_cursor'").fetchone()
            return row[0] if row else None
    except Exception:
        return None


def _set_sa_cursor(cursor: str | None) -> None:
    """Save the SA pagination cursor for next refresh."""
    with sqlite3.connect(DB_PATH) as con:
        if cursor:
            con.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                ("sa_cursor", cursor),
            )
        else:
            # Cursor is None = full cycle completed. Clear SA titles so next
            # cycle rebuilds from scratch (removes stale titles).
            con.execute("DELETE FROM meta WHERE key = 'sa_cursor'")
            con.execute("DELETE FROM sa_titles")
            logger.info("SA: Cleared accumulated SA titles (full cycle done, next refresh starts fresh)")


def _load_sa_titles_from_db() -> dict[str, dict]:
    """Load previously accumulated SA-only titles."""
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute("SELECT id, data FROM sa_titles").fetchall()
    return {r[0]: json.loads(r[1]) for r in rows}


def _save_sa_titles_to_db(sa_titles: dict[str, dict]) -> None:
    """Save accumulated SA titles for next refresh."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM sa_titles")
        if sa_titles:
            con.executemany(
                "INSERT INTO sa_titles (id, data) VALUES (?, ?)",
                [(sid, json.dumps(data)) for sid, data in sa_titles.items()],
            )


# ---------------------------------------------------------------------------
# In-memory cache (loaded from DB on startup, updated after refresh)
# ---------------------------------------------------------------------------

_cache: dict = {"titles": [], "counts": {}, "updated_at": 0.0}
_refresh_status: dict = {"running": False, "error": None}


async def _refresh_and_persist() -> None:
    """Fetch from both APIs, update DB and in-memory cache."""
    _refresh_status["error"] = None
    try:
        logger.info("Refreshing from JustWatch + Streaming Availability APIs...")
        start = time.time()
        titles, counts = await _fetch_all()
        if not titles:
            raise RuntimeError("Fetch returned 0 titles — keeping existing data")
        await _download_all_images(titles)
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


async def _backfill_images() -> None:
    """Download images for cached titles that still have external URLs (first deploy)."""
    titles = _cache["titles"]
    need_download = [t for t in titles if any(
        (t.get(k) or "").startswith("http") for k in ("posterUrl", "backdropUrl")
    )]
    if not need_download:
        logger.info("Backfill: all images already local")
        return
    logger.info("Backfill: downloading images for %d titles", len(need_download))
    await _download_all_images(need_download)
    await asyncio.to_thread(_save_to_db, _cache["titles"], _cache["counts"])
    logger.info("Backfill: complete, DB updated with local paths")


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
    if titles and any(
        (t.get("posterUrl") or "").startswith("http")
        or (t.get("backdropUrl") or "").startswith("http")
        for t in titles
    ):
        asyncio.create_task(_backfill_images())
    yield


app = FastAPI(title="Dutch Audio Streaming Finder", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/robots.txt")
async def robots():
    return FileResponse(STATIC_DIR / "robots.txt", media_type="text/plain")


@app.get("/sitemap.xml")
async def sitemap():
    return FileResponse(STATIC_DIR / "sitemap.xml", media_type="application/xml")


@app.get("/api/titles")
async def api_titles():
    return _cache["titles"]


@app.post("/api/refresh")
async def api_refresh(request: Request):
    # Auth: either CSRF header (browser) or secret token (cron)
    token = request.headers.get("x-refresh-secret", "")
    is_cron = REFRESH_SECRET and token == REFRESH_SECRET
    if not is_cron and not request.headers.get("x-requested-with"):
        raise HTTPException(status_code=403, detail="Forbidden")

    if _refresh_status["running"]:
        return {"status": "already_running"}

    # Cooldown (cron bypasses)
    if not is_cron and _cache["updated_at"] and time.time() - _cache["updated_at"] < REFRESH_COOLDOWN:
        remaining = int(REFRESH_COOLDOWN - (time.time() - _cache["updated_at"]))
        hours = remaining // 3600
        mins = (remaining % 3600) // 60
        return {"status": "cooldown", "retry_after": remaining,
                "message": f"Next refresh available in {hours}h {mins}m"}

    # Set flag synchronously before yielding control — prevents race condition
    _refresh_status["running"] = True
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
