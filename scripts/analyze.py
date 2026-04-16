"""Enrich reviews (sentiment, week) and generate weekly insights via Claude API."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

OUTPUTS_DIR = Path("outputs")

POS_WORDS = {"great", "clean", "friendly", "perfect", "excellent", "love", "amazing",
             "helpful", "comfortable", "recommend", "nice", "good"}
NEG_WORDS = {"dirty", "rude", "broken", "noisy", "bad", "terrible", "worst", "mold",
             "roach", "bug", "smell", "awful", "disgusting"}


def sentiment(text: str) -> str:
    t = (text or "").lower()
    pos = sum(1 for w in POS_WORDS if w in t)
    neg = sum(1 for w in NEG_WORDS if w in t)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def iso_week(date_str: str) -> str:
    try:
        d = datetime.fromisoformat(date_str)
        return f"{d.isocalendar().year}-W{d.isocalendar().week:02d}"
    except (ValueError, TypeError):
        return ""


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sentiment"] = df["text"].apply(sentiment)
    df["week"] = df["date"].apply(iso_week)
    return df


# ---------- Claude API analysis ----------

def _claude_call(prompt: str, model: str = "claude-sonnet-4-6") -> str:
    """Call Claude API. Requires ANTHROPIC_API_KEY env var and `anthropic` package."""
    from anthropic import Anthropic
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    resp = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text


def _chunks(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def analyze_batch(reviews: list[dict]) -> dict:
    """Send a batch to Claude and get structured insights."""
    sample = [{"source": r["source"], "rating": r.get("rating"), "text": r["text"]}
              for r in reviews]
    prompt = (
        "You are analyzing hotel guest reviews. Return ONLY a JSON object with keys "
        "`top_issues` (array of {name, frequency}) and `top_strengths` (array of "
        "{name, frequency}). Each array: exactly 5 items, frequency as 0..1 float, "
        "sorted desc. No prose.\n\nReviews:\n"
        + json.dumps(sample, ensure_ascii=False)
    )
    try:
        return json.loads(_claude_call(prompt))
    except Exception as e:
        print(f"[analyze] Claude call failed: {e}")
        return {"top_issues": [], "top_strengths": []}


def _merge_batches(results: list[dict]) -> dict:
    """Average frequencies across batches, keep top 5 of each."""
    def agg(key: str) -> list[dict]:
        pool: dict[str, list[float]] = {}
        for r in results:
            for item in r.get(key, []):
                pool.setdefault(item["name"], []).append(float(item.get("frequency", 0)))
        ranked = sorted(
            [{"name": k, "frequency": round(sum(v) / len(v), 3)} for k, v in pool.items()],
            key=lambda x: x["frequency"], reverse=True,
        )
        return ranked[:5]

    return {"top_issues": agg("top_issues"), "top_strengths": agg("top_strengths")}


def weekly_insights(df: pd.DataFrame, week: str | None = None,
                    batch_size: int = 25, out_dir: Path = OUTPUTS_DIR) -> Path:
    """Build insights_weekly.json for the given ISO week (default: latest)."""
    out_dir.mkdir(parents=True, exist_ok=True)

    if week is None:
        week = df["week"].max() if "week" in df.columns else ""
    scope = df[df["week"] == week] if week else df

    ratings = pd.to_numeric(scope["rating"], errors="coerce")
    reviews = scope.to_dict(orient="records")

    batch_results = []
    if reviews and os.environ.get("ANTHROPIC_API_KEY"):
        for batch in _chunks(reviews, batch_size):
            batch_results.append(analyze_batch(batch))
    merged = _merge_batches(batch_results) if batch_results else {"top_issues": [], "top_strengths": []}

    payload = {
        "week": week,
        "top_issues": merged["top_issues"],
        "top_strengths": merged["top_strengths"],
        "kpis": {
            "avg_rating": round(float(ratings.mean()), 2) if ratings.notna().any() else None,
            "total_reviews": int(len(scope)),
        },
    }

    path = out_dir / "insights_weekly.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return path
