# backend/app/etl/travelagency_etl.py
"""
Travel agency ETL: clean_file(path) and process_travelagency_upload(upload_id, raw, run_id=None)

Normalized cleaned fields:
 - agencykey
 - agencyname
 - transactionid
 - passengername
 - flightnumber
 - saleamount (numeric)
 - currency
 - saledate (date or None)
 - rawjson
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

from ..services.supabase_client import sb

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
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    df = _read_csv_file(p)
    df = _normalize_columns(df)

    # map common keys
    if "transaction_id" not in df.columns and "transactionid" in df.columns:
        pass
    if "transaction_id" in df.columns and "transactionid" not in df.columns:
        df = df.rename(columns={"transaction_id": "transactionid"})
    if "agency_id" in df.columns and "agencykey" not in df.columns:
        df = df.rename(columns={"agency_id": "agencykey"})
    if "agency_name" in df.columns and "agencyname" not in df.columns:
        df = df.rename(columns={"agency_name": "agencyname"})
    if "sale_date" in df.columns and "saledate" not in df.columns:
        df = df.rename(columns={"sale_date": "saledate"})

    if "saledate" in df.columns:
        df["saledate"] = pd.to_datetime(df["saledate"], errors="coerce").dt.date

    if "sale_amount" in df.columns and "saleamount" not in df.columns:
        df = df.rename(columns={"sale_amount": "saleamount"})

    # normalize string columns
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})

    df = df.drop_duplicates().reset_index(drop=True)
    records = _df_to_records(df)
    cleaned_rows = []
    raw_rows = []
    for r in records:
        raw_rows.append({"rawjson": r})
        cleaned_rows.append({
            "agencykey": r.get("agencykey") or r.get("agency_id"),
            "agencyname": r.get("agencyname") or r.get("agency_name"),
            "transactionid": r.get("transactionid") or r.get("transaction_id"),
            "passengername": r.get("passengername") or r.get("passenger_name"),
            "flightnumber": r.get("flightnumber") or r.get("flight_number"),
            "saleamount": r.get("saleamount") if r.get("saleamount") not in (None, "", "nan") else None,
            "currency": r.get("currency"),
            "saledate": r.get("saledate"),
            "rawjson": r
        })
    return cleaned_rows, raw_rows

def _upsert_travel_row(row: Dict[str, Any], upload_id: int):
    payload = {
        "agencykey": row.get("agencykey"),
        "agencyname": row.get("agencyname"),
        "transactionid": row.get("transactionid"),
        "passengername": row.get("passengername"),
        "flightnumber": row.get("flightnumber"),
        "saleamount": row.get("saleamount"),
        "currency": row.get("currency"),
        "saledate": row.get("saledate"),
        "rawjson": row.get("rawjson") if "rawjson" in row else row,
        "upload_id": upload_id,
        "processed": True,
        "insertedat": datetime.utcnow().isoformat(),
        "error_count": 0,
        "last_error": None
    }
    res = supabase.table("cleaned_travelagency").upsert(payload, on_conflict="transactionid").execute()
    if getattr(res, "status_code", None) and res.status_code >= 400:
        raise Exception(res.error)

def process_travelagency_upload(upload_id: int, raw: Optional[Dict[str, Any]], run_id: int = None) -> Dict[str, int]:
    rows = []
    if not raw:
        return {"processed": 0, "errors": 0}
    if isinstance(raw, dict) and "rows" in raw and isinstance(raw["rows"], list):
        rows = raw["rows"]
    elif isinstance(raw, dict) and "raw_rows" in raw and isinstance(raw["raw_rows"], list):
        rows = [r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r for r in raw["raw_rows"]]
    elif isinstance(raw, list):
        rows = raw

    processed = 0
    errors = 0
    for r in rows:
        try:
            rec = r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r
            transaction = rec.get("transactionid") or rec.get("transaction_id") or rec.get("transaction")
            if not transaction:
                errors += 1
                supabase.table("import_errors").insert({"upload_id": upload_id, "row_data": rec, "message": "missing transaction id"}).execute()
                continue
            normalized = {
                "agencykey": rec.get("agencykey") or rec.get("agency_id"),
                "agencyname": rec.get("agencyname") or rec.get("agency_name"),
                "transactionid": transaction,
                "passengername": rec.get("passengername") or rec.get("passenger_name"),
                "flightnumber": rec.get("flightnumber") or rec.get("flight_number"),
                "saleamount": float(rec.get("saleamount")) if rec.get("saleamount") not in (None, "", "nan") else None,
                "currency": rec.get("currency"),
                "saledate": rec.get("saledate"),
                "rawjson": rec
            }
            _upsert_travel_row(normalized, upload_id)
            processed += 1
        except Exception as e:
            errors += 1
            supabase.table("import_errors").insert({"upload_id": upload_id, "row_data": r, "message": str(e)}).execute()
    return {"processed": processed, "errors": errors}

# quick local test
if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "sample_travelagency.csv"
    cleaned, raw = clean_file(p)
    print("Cleaned rows:", len(cleaned))
    print(cleaned[:3])
