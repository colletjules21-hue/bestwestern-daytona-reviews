"""Unified schema for hotel reviews across Google, Booking, TripAdvisor."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional


@dataclass
class Review:
    review_id: str
    source: str           # "google" | "booking" | "tripadvisor"
    date: str             # ISO date "YYYY-MM-DD"
    rating: Optional[float]
    text: str
    author: str
    url: str
    lang: str = "en"

    def to_dict(self) -> dict:
        return asdict(self)


def make_review_id(source: str, date: str, author: str, text: str) -> str:
    """Deterministic hash so reruns don't create duplicates."""
    payload = f"{source}|{date}|{author}|{text[:200]}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:16]


def normalize_date(value) -> str:
    """Parse any reasonable date string into ISO YYYY-MM-DD. Empty on failure."""
    if not value:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%B %Y", "%b %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return ""


EXAMPLE = {
    "review_id": "a1b2c3d4e5f60718",
    "source": "booking",
    "date": "2026-03-14",
    "rating": 8.0,
    "text": "Clean room, friendly staff, breakfast was average.",
    "author": "Alice",
    "url": "https://booking.com/hotel/x/reviews#r123",
    "lang": "en",
}
