# Dutch Audio Streaming Finder

Browse movies & series with Dutch audio across Netflix, Disney+, Prime Video, Apple TV+, VRT MAX & GoPlay in Belgium.

Live at [streaming.ma.ttias.be](https://streaming.ma.ttias.be).

![Screenshot](static/og-image.png)

## Why

There's no way to browse Dutch-audio content across streaming platforms. Netflix has `/browse/audio` but Disney+, Prime, and Apple TV+ don't, and local platforms like VRT MAX and GoPlay are separate silos. This app aggregates them all into a single browsing experience for Belgium (BE).

Data comes from three sources: [JustWatch](https://www.justwatch.com/)'s GraphQL API (primary, including VRT MAX), the [Streaming Availability API](https://www.movieofthenight.com/about/api) (fills gaps where JustWatch lacks audio language data), and [GoPlay](https://www.play.tv/)'s public API (free Belgian content). Titles are filtered for Dutch audio, deduplicated across platforms, and stored locally. The frontend is a single HTML file with no build step.

## Vibe coded

This was mostly vibe coded with Claude. The goal was a functional browsing tool, not a production-grade application. It works, it's useful, fork it if you want to adapt it for your country or language.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env and add your RAPIDAPI_KEY and REFRESH_SECRET
```

## Run

```bash
uvicorn backend:app --port 8000
```

Open [http://localhost:8000](http://localhost:8000). On first run the database is empty -- click the refresh button to fetch titles from JustWatch (~40 seconds).

## How it works

**Backend** (`backend.py`): Python + FastAPI. Fetches from three sources:
- **JustWatch** (GraphQL, no key needed): up to 2000 titles per provider (Netflix, Disney+, Prime Video, Apple TV+, VRT MAX), full rescan each refresh
- **Streaming Availability** (RapidAPI, free tier): fills audio language gaps, paginates incrementally across refreshes (~80 pages/day, full catalog in ~5 days)
- **GoPlay** (public REST API, no key needed): free Belgian content from Play/Play Crime/Play Reality, filtered for Dutch audio

Titles are filtered for Dutch audio, deduplicated across sources, and stored in SQLite. Affiliate tracking URLs are stripped from deeplinks.

**Frontend** (`static/index.html`): Single HTML file with embedded CSS & JS. Dark theme, responsive poster grid, provider toggle pills, movie/show filter, search. Click a poster for synopsis, IMDb score, and direct deeplinks to the streaming platform. All filtering is client-side after initial load. Provider selections are saved in localStorage.

**Refresh**: The refresh button fetches fresh data in the background. Limited to once per 23 hours. The server starts instantly from the SQLite database, so it works even if either API is down.

## Deployment

Included config files for deployment behind Caddy as a reverse proxy:

- `streaming.ma.ttias.be.conf` -- Caddy site config
- `streaming-ma-ttias-be.service` -- systemd unit file (runs as `www-data`)

## Daily refresh (cron)

Set up a daily cron job to keep the catalog fresh:

```bash
# Generate a secret and add it to .env on the server
echo "REFRESH_SECRET=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')" >> .env

# Add to crontab (e.g. daily at 4am)
0 4 * * * curl -s -X POST -H "X-Refresh-Secret: YOUR_SECRET_HERE" https://your-domain/api/refresh > /dev/null 2>&1
```

Each refresh:
- **JustWatch**: full rescan (~40s) — titles removed from platforms disappear immediately
- **Streaming Availability**: resumes from where it left off (80 pages/day on free tier). When the full catalog is scanned (~5 days), it resets and starts a fresh cycle, dropping any stale titles.

The secret bypasses the cooldown and CSRF check, so the cron can run independently of manual refreshes.

## Fork it for your country

This app filters by **country + audio language**. You can adapt it for German audio in Germany, French audio in Switzerland, etc. Here's what to change:

### backend.py

**1. Country and language** (lines ~35-36):

```python
COUNTRY = "DE"      # was "BE" — JustWatch country code
LANGUAGE = "de"     # was "nl" — JustWatch language for metadata
```

**2. Audio language filter** — search for `"nl"` in these functions and replace with your [ISO 639-1 code](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes):

- `_jw_has_dutch_audio()` — rename to e.g. `_jw_has_german_audio()`, change `"nl"` to `"de"`
- `_jw_transform_title()` — same `"nl"` check in the offers loop
- `_sa_has_dutch_audio()` — rename, change `("nld", "dut")` to your [ISO 639-3 codes](https://en.wikipedia.org/wiki/List_of_ISO_639-3_codes) (e.g. `("deu", "ger")` for German)
- `_sa_transform_show()` — same language codes in the audios check

**3. Streaming Availability country** (line ~578):

```python
"country": "de",    # was "be"
```

Also update `_sa_has_dutch_audio` and `_sa_transform_show` where they access `.get("be", [])` — change to your country code:

```python
show.get("streamingOptions", {}).get("de", [])   # was .get("be", [])
```

**4. Providers** — the available platforms differ by country. Update these dicts to match what's available in yours:

- `PROVIDERS` — JustWatch provider short names + display config. Find yours at [JustWatch](https://www.justwatch.com/) by checking what's available in your country.
- `SA_CATALOGS` — Streaming Availability catalog IDs (see [their docs](https://docs.movieofthenight.com/resource/shows))
- `SA_SERVICE_MAP` — maps SA service IDs to your provider short names
- `SA_PROVIDER_ICONS` / `SA_PROVIDER_NAMES` — display names and icon URLs
- `PROVIDER_ICON_FILENAMES` — local icon filenames

**5. App title** (line ~903):

```python
app = FastAPI(title="German Audio Streaming Finder", ...)
```

### static/index.html

**6. HTML metadata** — update these at the top of the file:

- `<html lang="de">` — your language code
- `<title>` and all `<meta>` tags — your title, description, OG tags
- `og:url` and `og:image` — your domain
- `<h1>` — your heading
- Loading text — "Loading German audio titles..."
- `img.alt` suffix in the JS — change `"Dutch audio streaming"` to yours

**7. Provider pills** — update the JS config to match your providers:

```javascript
var DEFAULT_PROVIDERS = ['nfx', 'dnp', 'prv', 'atp'];
var PROVIDER_CONFIG = [
    { short: 'nfx', label: 'Netflix' },
    { short: 'dnp', label: 'Disney+' },
    // ... your providers
];
```

**8. Pill colors** — the CSS has provider-specific colors (`.pill[data-provider="nfx"]`). Add/remove rules for your providers.

### Other files

- **`.env`** — get your own `RAPIDAPI_KEY` from [Streaming Availability](https://www.movieofthenight.com/about/api) and set a `REFRESH_SECRET`
- **`streaming.ma.ttias.be.conf`** — rename and update the domain
- **`static/og-image.png`** — replace with your own
- **`static/sitemap.xml`** / **`static/robots.txt`** — update the domain

### Quick checklist

```
grep -rn '"nl"' backend.py          # should return nothing after your changes
grep -rn '"be"' backend.py          # should return nothing
grep -rn 'Dutch' backend.py static/ # should return nothing
grep -rn 'Belgium' static/          # should return nothing
```

## License

MIT
