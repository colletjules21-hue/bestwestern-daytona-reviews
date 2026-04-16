"""Push weekly insights into a Notion database (upsert by week + issue).

Schema expected in the Notion DB:
    - Week        (title)          ISO week, e.g. "2026-W16"
    - Issue       (rich_text)      Name of the issue / strength
    - Type        (select)         "Issue" | "Strength"
    - Source      (multi_select)   "Google" | "Booking" | "TripAdvisor"
    - Frequency   (number, %)      0..1 float
    - Mentions    (number)         integer count
    - Trend       (select)         "Up" | "Down" | "Stable" | "New"
    - Status      (select)         "Open" | "In Progress" | "Fixed" | "Won't Fix"
    - Priority    (select)         "High" | "Medium" | "Low"
    - Week Date   (date)           Monday of the ISO week
    - Example Quote (rich_text)
    - Notes       (rich_text)
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['NOTION_TOKEN']}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _iso_week_to_monday(week: str) -> str | None:
    """Convert 'YYYY-Www' to the Monday of that ISO week as YYYY-MM-DD."""
    try:
        year_s, week_s = week.split("-W")
        d = date.fromisocalendar(int(year_s), int(week_s), 1)
        return d.isoformat()
    except (ValueError, AttributeError):
        return None


def _find_page(database_id: str, week: str, issue: str) -> str | None:
    """Return page_id if a row already exists for (Week, Issue)."""
    r = requests.post(
        f"{NOTION_API}/databases/{database_id}/query",
        headers=_headers(),
        json={
            "filter": {
                "and": [
                    {"property": "Week", "title": {"equals": week}},
                    {"property": "Issue", "rich_text": {"equals": issue}},
                ]
            }
        },
        timeout=30,
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None


def _build_props(
    week: str,
    issue: str,
    item_type: str,
    frequency: float,
    mentions: int | None = None,
    sources: list[str] | None = None,
    trend: str = "New",
    status: str = "Open",
    priority: str | None = None,
    example_quote: str | None = None,
) -> dict:
    """Build the Notion properties payload matching the DB schema (case-sensitive)."""
    props: dict = {
        "Week": {"title": [{"text": {"content": week}}]},
        "Issue": {"rich_text": [{"text": {"content": issue}}]},
        "Type": {"select": {"name": item_type}},
        "Frequency": {"number": round(float(frequency), 3)},
        "Trend": {"select": {"name": trend}},
        "Status": {"select": {"name": status}},
    }
    if mentions is not None:
        props["Mentions"] = {"number": int(mentions)}
    if sources:
        props["Source"] = {"multi_select": [{"name": s} for s in sources]}
    if priority:
        props["Priority"] = {"select": {"name": priority}}
    if example_quote:
        props["Example Quote"] = {"rich_text": [{"text": {"content": example_quote[:1800]}}]}
    monday = _iso_week_to_monday(week)
    if monday:
        props["Week Date"] = {"date": {"start": monday}}
    return props


def _upsert(database_id: str, props: dict, week: str, issue: str) -> dict:
    page_id = _find_page(database_id, week, issue)
    if page_id:
        r = requests.patch(
            f"{NOTION_API}/pages/{page_id}",
            headers=_headers(),
            json={"properties": props},
            timeout=30,
        )
    else:
        r = requests.post(
            f"{NOTION_API}/pages",
            headers=_headers(),
            json={
                "parent": {"database_id": database_id},
                "properties": props,
            },
            timeout=30,
        )
    r.raise_for_status()
    return r.json()


def _priority_from_frequency(freq: float) -> str:
    if freq >= 0.25:
        return "High"
    if freq >= 0.10:
        return "Medium"
    return "Low"


def push(insights_path: Path | str, database_id: str | None = None) -> int:
    """Upsert one row per issue / strength for the week into the Notion DB."""
    database_id = database_id or os.environ["NOTION_DATABASE_ID"]
    data = json.loads(Path(insights_path).read_text())
    week = data.get("week", "")
    if not week:
        raise ValueError("insights JSON must contain a non-empty 'week' field")

    count = 0

    for item in data.get("top_issues", []):
        freq = float(item.get("frequency", 0) or 0)
        props = _build_props(
            week=week,
            issue=item["name"],
            item_type="Issue",
            frequency=freq,
            mentions=item.get("mentions"),
            sources=item.get("sources"),
            trend=item.get("trend", "New"),
            status="Open",
            priority=_priority_from_frequency(freq),
            example_quote=item.get("example"),
        )
        _upsert(database_id, props, week, item["name"])
        count += 1

    for item in data.get("top_strengths", []):
        freq = float(item.get("frequency", 0) or 0)
        props = _build_props(
            week=week,
            issue=item["name"],
            item_type="Strength",
            frequency=freq,
            mentions=item.get("mentions"),
            sources=item.get("sources"),
            trend=item.get("trend", "Stable"),
            status="Open",
            priority="Low",
            example_quote=item.get("example"),
        )
        _upsert(database_id, props, week, item["name"])
        count += 1

    return count
