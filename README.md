# Dutch Audio Streaming Finder

Browse movies & series with Dutch audio across Netflix, Disney+, Prime Video & Apple TV+ in Belgium.

Live at [streaming.ma.ttias.be](https://streaming.ma.ttias.be).

![Screenshot](static/og-image.png)

## Why

There's no way to browse Dutch-audio content across streaming platforms. Netflix has `/browse/audio` but Disney+, Prime, and Apple TV+ don't. This app aggregates all four into a single browsing experience for Belgium (BE).

Data comes from [JustWatch](https://www.justwatch.com/)'s GraphQL API. Titles are fetched, filtered for Dutch audio, deduplicated across platforms, and stored locally. The frontend is a single HTML file with no build step.

## Vibe coded

This was mostly vibe coded with Claude. The goal was a functional browsing tool, not a production-grade application. It works, it's useful, fork it if you want to adapt it for your country or language.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
uvicorn backend:app --port 8000
```

Open [http://localhost:8000](http://localhost:8000). On first run the database is empty -- click the refresh button to fetch titles from JustWatch (~40 seconds).

## How it works

**Backend** (`backend.py`): Python + FastAPI. Makes direct GraphQL calls to JustWatch's API, fetches up to 2000 titles per provider (Netflix, Disney+, Prime Video, Apple TV+), filters for titles with `"nl"` in `audioLanguages`, deduplicates across platforms, and stores results in a local SQLite database. Affiliate tracking URLs are stripped from deeplinks.

**Frontend** (`static/index.html`): Single HTML file with embedded CSS & JS. Dark theme, responsive poster grid, provider toggle pills, movie/show filter, search. Click a poster for synopsis, IMDb score, and direct deeplinks to the streaming platform. All filtering is client-side after initial load.

**Refresh**: The refresh button fetches fresh data from JustWatch in the background. Limited to once per 24 hours. The server starts instantly from the SQLite database, so it works even if JustWatch is down.

## Deployment

Included config files for deployment behind Caddy as a reverse proxy:

- `streaming.ma.ttias.be.conf` -- Caddy site config
- `streaming-ma-ttias-be.service` -- systemd unit file (runs as `www-data`)

## License

MIT
