"""Load JSON review files from data/raw and standardize them into the unified schema."""

import json
from pathlib import Path

from scripts.schema import Review, make_review_id, normalize_date

RAW_DIR = Path("data/raw")

# Column variants seen across Google / Booking / TripAdvisor exports
FIELD_ALIASES = {
    "text": ["text", "body", "content", "review", "comment", "positive"],
    "rating": ["rating", "score", "stars"],
    "date": ["date", "stayDate", "review_date", "publishedAt", "createdAt"],
    "author": ["author", "guest", "user", "reviewer", "name"],
    "url": ["url", "link", "permalink"],
    "lang": ["lang", "language"],
}


def _pick(row: dict, field: str, default=""):
    for key in FIELD_ALIASES[field]:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _to_float(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def load_source(path: Path) -> list[Review]:
    """Load one JSON file (list of raw review dicts) and return standardized Review list."""
    source = path.stem.replace("reviews_", "").replace("_reviews", "")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw = raw.get("reviews", [])

    reviews: list[Review] = []
    for row in raw:
        text = str(_pick(row, "text")).strip()
        if not text:
            continue
        date = normalize_date(_pick(row, "date"))
        author = str(_pick(row, "author", "anonymous")).strip()
        reviews.append(Review(
            review_id=_pick(row, "url") and f"{source}_{abs(hash(_pick(row, 'url'))) % 10**12}"
                      or make_review_id(source, date, author, text),
            source=source,
            date=date,
            rating=_to_float(_pick(row, "rating")),
            text=text,
            author=author,
            url=str(_pick(row, "url")),
            lang=str(_pick(row, "lang", "en")),
        ))
    return dedup(reviews)


def dedup(reviews: list[Review]) -> list[Review]:
    """Keep first occurrence per review_id."""
    seen: set[str] = set()
    out: list[Review] = []
    for r in reviews:
        if r.review_id in seen:
            continue
        seen.add(r.review_id)
        out.append(r)
    return out


def load_all(raw_dir: Path = RAW_DIR) -> list[Review]:
    """Load every JSON file in raw_dir and merge into one deduped list."""
    all_reviews: list[Review] = []
    for path in sorted(raw_dir.glob("*.json")):
        all_reviews.extend(load_source(path))
    return dedup(all_reviews)
