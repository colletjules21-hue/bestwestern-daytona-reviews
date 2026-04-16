"""Merge all source reviews into a master dataset (CSV + JSON)."""

from pathlib import Path
import json
import pandas as pd

from scripts.schema import Review

PROCESSED_DIR = Path("data/processed")


def to_dataframe(reviews: list[Review]) -> pd.DataFrame:
    return pd.DataFrame([r.to_dict() for r in reviews])


def merge_master(reviews: list[Review], out_dir: Path = PROCESSED_DIR) -> dict[str, Path]:
    """Deduplicate, sort by date desc, and save CSV + JSON."""
    out_dir.mkdir(parents=True, exist_ok=True)

    df = to_dataframe(reviews)
    if df.empty:
        return {}

    df = df.drop_duplicates(subset=["review_id"])
    df = df.sort_values("date", ascending=False).reset_index(drop=True)

    csv_path = out_dir / "reviews_master.csv"
    json_path = out_dir / "reviews_master.json"

    df.to_csv(csv_path, index=False)
    json_path.write_text(json.dumps(df.to_dict(orient="records"), ensure_ascii=False, indent=2))

    return {"csv": csv_path, "json": json_path, "rows": len(df)}
