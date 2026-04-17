"""Enrich reviews (sentiment, week) and generate weekly insights via Claude API."""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

OUTPUTS_DIR = Path("outputs")

POS_WORDS = {"great", "clean", "friendly", "perfect", "excellent", "love", "amazing",
             "helpful", "comfortable", "recommend", "nice", "good", "beautiful", "wonderful",
             "fantastic", "best", "staff", "breakfast", "fresh", "spacious", "cozy"}
NEG_WORDS = {"dirty", "rude", "broken", "noisy", "bad", "terrible", "worst", "mold",
             "roach", "bug", "smell", "awful", "disgusting", "awful", "cockroach",
             "impaired", "unsafe", "chaos", "slow", "musty", "stain", "lights"}


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
        d = datetime.fromisoformat(str(date_str).strip())
        return f"{d.isocalendar().year}-W{d.isocalendar().week:02d}"
    except (ValueError, TypeError):
        return ""


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sentiment"] = df["text"].apply(sentiment)
    df["week"] = df["date"].apply(iso_week)
    return df


# ---------- Claude API analysis ----------

MODELS_TO_TRY = [
    "claude-haiku-4-5-20251001",
    "claude-3-5-haiku-20241022",
    "claude-3-haiku-20240307",
]


def _claude_call(prompt: str) -> str:
    """Try Claude models in order until one works."""
    from anthropic import Anthropic
    client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    last_err = None
    for model in MODELS_TO_TRY:
        try:
            resp = client.messages.create(
                model=model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.content[0].text
        except Exception as e:
            last_err = e
            print(f"[analyze] Model {model} failed: {e}")
    raise last_err


def _chunks(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _keyword_analysis(reviews: list[dict]) -> dict:
    """Fallback: keyword-based issue/strength extraction."""
    issue_patterns = {
        "Cockroaches / pest infestation": ["cockroach", "roach", "bug", "pest"],
        "Cleanliness issues (mold, stains, smell)": ["mold", "musty", "stain", "dirty", "smell", "mildew"],
        "Maintenance problems (lighting, broken)": ["lights", "light", "broken", "maintenance", "repair"],
        "Unprofessional staff conduct": ["impaired", "rude", "unsafe", "unprofessional", "refused"],
        "Noisy A/C unit": ["noisy", "a/c", "air condition", "ac unit", "noise"],
        "Slow check-in process": ["slow", "check-in", "wait", "waited", "checkin"],
        "Parking issues": ["parking", "lot full", "tight parking"],
    }
    strength_patterns = {
        "Friendly and helpful staff": ["friendly", "helpful", "kind", "amazing staff", "staff was", "incredible"],
        "Breakfast quality and variety": ["breakfast", "fresh fruit", "hot options", "variety"],
        "Great location and beach access": ["location", "beach", "ocean", "view", "access"],
        "Room cleanliness": ["clean", "spotless", "tidy", "neat"],
        "Comfortable rooms and amenities": ["comfortable", "spacious", "balcony", "pool", "amenities"],
    }
    n = len(reviews)
    if n == 0:
        return {"top_issues": [], "top_strengths": []}

    def score_patterns(patterns):
        results = []
        for name, keywords in patterns.items():
            count = sum(
                1 for r in reviews
                if any(kw.lower() in r.get("text", "").lower() for kw in keywords)
            )
            if count > 0:
                results.append({"name": name, "frequency": round(count / n, 3), "mentions": count})
        return sorted(results, key=lambda x: x["frequency"], reverse=True)[:5]

    return {
        "top_issues": score_patterns(issue_patterns),
        "top_strengths": score_patterns(strength_patterns),
    }


def analyze_batch(reviews: list[dict]) -> dict:
    """Send a batch to Claude and get structured insights. Falls back to keyword analysis."""
    sample = [{"source": r.get("source", ""), "rating": r.get("rating"), "text": r.get("text", "")}
              for r in reviews]
    prompt = (
        "You are analyzing hotel guest reviews. Return ONLY a JSON object with keys "
        "`top_issues` (array of {name, frequency, mentions}) and `top_strengths` (array of "
        "{name, frequency, mentions}). Each array: exactly 5 items, frequency as 0..1 float, "
        "sorted desc. No prose.\n\nReviews:\n"
        + json.dumps(sample, ensure_ascii=False)
    )
    try:
        raw = _claude_call(prompt)
        result = json.loads(raw)
        if result.get("top_issues") or result.get("top_strengths"):
            return result
        raise ValueError("Empty result from Claude")
    except Exception as e:
        print(f"[analyze] Claude call failed ({e}), using keyword fallback")
        return _keyword_analysis(reviews)


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
        valid_weeks = df["week"].dropna().replace("", float("nan")).dropna() if "week" in df.columns else pd.Series(dtype=str)
        week = valid_weeks.max() if not valid_weeks.empty else ""
    scope = df[df["week"] == week] if week else df

    ratings = pd.to_numeric(scope["rating"], errors="coerce")
    reviews = scope.to_dict(orient="records")

    batch_results = []
    for batch in _chunks(reviews, batch_size):
        batch_results.append(analyze_batch(batch))
    merged = _merge_batches(batch_results) if batch_results else _keyword_analysis(reviews)

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
