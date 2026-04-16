# hotel-reviews-pipeline

Weekly pipeline: Google / Booking / TripAdvisor reviews → unified dataset → Claude-powered insights → Notion dashboard.

## Flow

```
scrape (Claude in Chrome) → /data/raw/*.json
                                 │
                                 ▼
scrape_loader.py → unified Review schema (dedup, date normalization)
                                 │
                                 ▼
merge.py → /data/processed/reviews_master.{csv,json}
                                 │
                                 ▼
analyze.py → enrich (sentiment, ISO week) + Claude batch analysis
           → /outputs/insights_weekly.json
                                 │
                                 ▼
notion_push.py → upsert into Notion database
```

## Structure

```
.
├── main.py
├── requirements.txt
├── .github/workflows/weekly.yml   # cron: every Monday 08:00 UTC
├── data/
│   ├── raw/         # drop google.json / booking.json / tripadvisor.json here
│   └── processed/   # reviews_master.csv + .json (auto-generated)
├── outputs/         # insights_weekly.json (auto-generated)
└── scripts/
    ├── schema.py         # Review dataclass + helpers
    ├── scrape_loader.py  # load + dedup
    ├── merge.py          # master dataset
    ├── analyze.py        # enrich + Claude insights
    └── notion_push.py    # Notion upsert
```

## Setup

```bash
pip install -r requirements.txt
```

## Input — expected JSON shape

Drop one file per source in `data/raw/`: `google.json`, `booking.json`, `tripadvisor.json`.
Each file is a JSON array of raw review objects. The loader accepts common field aliases:

- `text` / `body` / `content` / `review` / `comment` / `positive`
- `rating` / `score` / `stars`
- `date` / `stayDate` / `review_date` / `publishedAt` / `createdAt`
- `author` / `guest` / `user` / `reviewer` / `name`
- `url` / `link` / `permalink`

Missing fields are tolerated; dates are normalized to `YYYY-MM-DD`; duplicates are removed via `review_id` hash.

## Run

```bash
python main.py                          # full run
python main.py --incremental            # only reviews newer than last run
python main.py --notion                 # also push to Notion
python main.py --week 2026-W15 --notion # force specific ISO week
```

## Required env vars (for --notion or Claude analysis)

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export NOTION_TOKEN=secret_...
export NOTION_DATABASE_ID=...
```

## Notion database schema

Create a database with these properties (exact names, case-sensitive):

| Property    | Type       |
|-------------|------------|
| `issue`     | Title      |
| `week`      | Text       |
| `frequency` | Number     |
| `trend`     | Select     |
| `status`    | Select     |

Select options — `trend`: up, down, flat — `status`: open, in progress, fixed.

## GitHub Actions

The workflow in `.github/workflows/weekly.yml` runs every Monday. Add these repo secrets:

- `ANTHROPIC_API_KEY`
- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`

## Weekly workflow

1. Use Claude in Chrome to scrape each source → save `google.json`, `booking.json`, `tripadvisor.json` to `data/raw/`.
2. Commit + push to GitHub.
3. The Action runs, or trigger manually with `Run workflow`.
4. Notion dashboard updates automatically.
