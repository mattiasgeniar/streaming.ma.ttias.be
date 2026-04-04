# Dutch Audio Streaming Finder

Browse movies & series with Dutch audio across Netflix, Disney+, Prime Video & Apple TV+ in Belgium.

Live at [streaming.ma.ttias.be](https://streaming.ma.ttias.be).

![Screenshot](static/og-image.png)

## Why

There's no way to browse Dutch-audio content across streaming platforms. Netflix has `/browse/audio` but Disney+, Prime, and Apple TV+ don't. This app aggregates all four into a single browsing experience for Belgium (BE).

Data comes from two sources: [JustWatch](https://www.justwatch.com/)'s GraphQL API (primary) and the [Streaming Availability API](https://www.movieofthenight.com/about/api) (fills gaps where JustWatch lacks audio language data, especially Netflix). Titles are filtered for Dutch audio, deduplicated across platforms, and stored locally. The frontend is a single HTML file with no build step.

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

**Backend** (`backend.py`): Python + FastAPI. Fetches from two APIs:
- **JustWatch** (GraphQL, no key needed): up to 2000 titles per provider, full rescan each refresh
- **Streaming Availability** (RapidAPI, free tier): fills audio language gaps, paginates incrementally across refreshes (~80 pages/day, full catalog in ~5 days)

Titles are filtered for Dutch audio, deduplicated by IMDb ID, and stored in SQLite. Affiliate tracking URLs are stripped from deeplinks.

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

## License

MIT
