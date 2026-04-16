"""Pipeline orchestrator — load → merge → enrich → analyze → push."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from scripts import scrape_loader, merge, analyze, notion_push

STATE_FILE = Path("data/processed/.last_run.json")


def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def run(incremental: bool = False, push_to_notion: bool = False,
        week: str | None = None) -> None:
    reviews = scrape_loader.load_all()
    if not reviews:
        print("No reviews found in data/raw/", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {len(reviews)} reviews from data/raw/")

    merge_out = merge.merge_master(reviews)
    print(f"Master: {merge_out['rows']} rows → {merge_out['csv']}")

    df = pd.read_csv(merge_out["csv"], dtype=str)
    df = analyze.enrich(df)

    if incremental:
        state = _load_state()
        since = state.get("last_date", "")
        if since:
            df = df[df["date"] > since]
            print(f"Incremental: {len(df)} reviews since {since}")

    insights_path = analyze.weekly_insights(df, week=week)
    print(f"Insights → {insights_path}")

    if push_to_notion:
        n = notion_push.push(insights_path)
        print(f"Notion: upserted {n} issue row(s)")

    if not df.empty:
        _save_state({"last_date": df["date"].max()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hotel reviews pipeline")
    parser.add_argument("--incremental", action="store_true", help="Only process new reviews since last run")
    parser.add_argument("--notion", action="store_true", help="Push insights to Notion")
    parser.add_argument("--week", help="Target ISO week (YYYY-Www). Default: latest.")
    args = parser.parse_args()
    run(incremental=args.incremental, push_to_notion=args.notion, week=args.week)
