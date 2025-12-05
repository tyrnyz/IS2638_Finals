# backend/app/etl/passengers_etl.py
"""
Passengers ETL: clean_file(path) and process_passengers_upload(upload_id, raw, run_id=None)

Normalized cleaned fields:
 - passenger_id
 - name
 - age (int or null)
 - rawjson

Conventions:
 - Column names are normalized to snake_case
 - Accepts CSV files; attempts latin-1 fallback
 - clean_file returns (cleaned_rows, raw_rows)
 - process_passengers_upload accepts staging_raw.raw shapes:
     - {"rows": [...]}      (parsed rows)
     - {"raw_rows": [...]}  (list of {"rawjson": {...}})
     - list[...]            (list of dicts)
"""

from __future__ import annotations
from pathlib import Path
import io
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

from ..services.supabase_client import sb

# ---------- helpers (pandas-based parsing + normalization) ----------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = (
        df.columns.str.strip()
        .str.replace(r"([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.replace(r"[ \-]+", "_", regex=True)
        .str.lower()
    )
    df.columns = cols
    return df

def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

def _read_csv_file(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, engine="python")
    except Exception:
        return pd.read_csv(path, engine="python", encoding="latin-1")

def clean_file(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (cleaned_rows, raw_rows).
    cleaned_rows fields: passenger_id, name, age, rawjson
    raw_rows: [{"rawjson": {...}}, ...]
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    df = _read_csv_file(p)
    df = _normalize_columns(df)

    # map common variations
    if "first_name" in df.columns and "last_name" in df.columns and "name" not in df.columns:
        df["name"] = df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    if "id" in df.columns and "passenger_id" not in df.columns:
        df = df.rename(columns={"id": "passenger_id"})
    # Try to coerce numeric age
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
        # replace NaN with None for consistency
        df["age"] = df["age"].where(pd.notnull(df["age"]), None)

    # trim and normalize string columns
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})

    df = df.drop_duplicates().reset_index(drop=True)
    records = _df_to_records(df)

    cleaned_rows: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []

    for r in records:
        raw_rows.append({"rawjson": r})
        cleaned_rows.append({
            "passenger_id": r.get("passenger_id") or r.get("id") or None,
            "name": r.get("name") or ( (r.get("first_name") or "") + " " + (r.get("last_name") or "") ).strip() or None,
            "age": int(r["age"]) if (r.get("age") not in (None, "", "nan") and str(r.get("age")).isdigit()) else (int(float(r["age"])) if r.get("age") not in (None, "", "nan") else None) if r.get("age") is not None else None,
            "rawjson": r
        })

    return cleaned_rows, raw_rows

# ---------- upsert / ETL runtime functions ----------

def _upsert_passenger_row(row: Dict[str, Any], upload_id: int) -> None:
    """
    Upsert a single normalized passenger row into cleaned_passengers.
    Sets processed = true and upload_id for lineage.
    """
    payload = {
        "passenger_id": row.get("passenger_id"),
        "name": row.get("name"),
        "age": row.get("age"),
        "rawjson": row.get("rawjson") if "rawjson" in row else row,
        "raw_upload_id": upload_id,
        "processed": True,
        "insertedat": datetime.utcnow().isoformat(),
        "error_count": 0,
        "last_error": None
    }
    # upsert on passenger_id natural key
    res = supabase.table("cleaned_passengers").upsert(payload, on_conflict="passenger_id").execute()
    if getattr(res, "status_code", None) and res.status_code >= 400:
        raise Exception(res.error)

def process_passengers_upload(upload_id: int, raw: Optional[Dict[str, Any]], run_id: int = None) -> Dict[str, int]:
    """
    Entrypoint for dispatcher/CLI.
    - upload_id: staging_raw id
    - raw: staging_raw.raw (expected to be {"rows": [...]}, or {"raw_rows":[...]}, or a list)
    Returns: {"processed": n, "errors": m}
    """
    rows: List[Dict[str, Any]] = []

    if not raw:
        return {"processed": 0, "errors": 0}

    if isinstance(raw, dict) and "rows" in raw and isinstance(raw["rows"], list):
        rows = raw["rows"]
    elif isinstance(raw, dict) and "raw_rows" in raw and isinstance(raw["raw_rows"], list):
        # raw_rows contains {"rawjson": {...}} entries
        rows = [r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r for r in raw["raw_rows"]]
    elif isinstance(raw, list):
        rows = raw
    else:
        # fallback: find the first list value in the raw dict
        if isinstance(raw, dict):
            for v in raw.values():
                if isinstance(v, list):
                    rows = v
                    break

    processed = 0
    errors = 0

    for r in rows:
        try:
            rec = r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r
            pid = rec.get("passenger_id") or rec.get("id")
            if not pid:
                errors += 1
                supabase.table("import_errors").insert({
                    "upload_id": upload_id,
                    "row_data": rec,
                    "message": "missing passenger id"
                }).execute()
                continue

            normalized = {
                "passenger_id": pid,
                "name": rec.get("name") or ( (rec.get("first_name") or "") + " " + (rec.get("last_name") or "") ).strip() or None,
                "age": (int(rec.get("age")) if (rec.get("age") not in (None, "", "nan") and str(rec.get("age")).replace('.','',1).isdigit()) else None),
                "rawjson": rec
            }

            _upsert_passenger_row(normalized, upload_id)
            processed += 1

        except Exception as e:
            errors += 1
            # record import error for the row
            supabase.table("import_errors").insert({
                "upload_id": upload_id,
                "row_data": r,
                "message": str(e)
            }).execute()

    return {"processed": processed, "errors": errors}


# Allow quick local testing: python backend/app/etl/passengers_etl.py sample.csv
if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "sample_passengers.csv"
    cleaned, raw = clean_file(p)
    print("Cleaned rows:", len(cleaned))
    print(cleaned[:3])
