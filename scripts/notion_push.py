"""Push weekly insights into a Notion database (upsert by week)."""

import json
import os
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


def _find_page(database_id: str, week: str, issue: str) -> str | None:
    """Return page_id if a row already exists for (week, issue)."""
    r = requests.post(
        f"{NOTION_API}/databases/{database_id}/query",
        headers=_headers(),
        json={"filter": {"and": [
            {"property": "week", "rich_text": {"equals": week}},
            {"property": "issue", "title": {"equals": issue}},
        ]}},
        timeout=30,
    )
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0]["id"] if results else None


def _issue_props(week: str, issue: str, frequency: float, trend: str, status: str) -> dict:
    return {
        "issue": {"title": [{"text": {"content": issue}}]},
        "week": {"rich_text": [{"text": {"content": week}}]},
        "frequency": {"number": round(float(frequency), 3)},
        "trend": {"select": {"name": trend}},
        "status": {"select": {"name": status}},
    }


def _upsert(database_id: str, props: dict, week: str, issue: str) -> dict:
    page_id = _find_page(database_id, week, issue)
    if page_id:
        r = requests.patch(f"{NOTION_API}/pages/{page_id}",
                           headers=_headers(), json={"properties": props}, timeout=30)
    else:
        r = requests.post(f"{NOTION_API}/pages",
                          headers=_headers(),
                          json={"parent": {"database_id": database_id}, "properties": props},
                          timeout=30)
    r.raise_for_status()
    return r.json()


def push(insights_path: Path, database_id: str | None = None) -> int:
    """Upsert one row per issue for the week into the Notion DB."""
    database_id = database_id or os.environ["NOTION_DATABASE_ID"]
    data = json.loads(Path(insights_path).read_text())
    week = data.get("week", "")

    # trend = compare to previous insight if you persist history; default 'flat' for now.
    count = 0
    for item in data.get("top_issues", []):
        props = _issue_props(week, item["name"], item.get("frequency", 0), "flat", "open")
        _upsert(database_id, props, week, item["name"])
        count += 1
    return count
