# backend/app/etl/flights_etl.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime
from ..services.supabase_client import sb

# helpers
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

# cleaning function
def clean_file(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Read file and return (cleaned_rows, raw_rows).
    cleaned_rows have keys:
      - flightkey
      - originairportkey
      - destinationairportkey
      - aircrafttype
      - rawjson
    raw_rows: list of {"rawjson": {...}}
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    df = _read_csv_file(p)
    df = _normalize_columns(df)

    # common header variants -> canonical names expected by cleaned_flights
    # map variants to your canonical keys
    rename_map = {}
    if "flight_number" in df.columns and "flightkey" not in df.columns:
        rename_map["flight_number"] = "flightkey"
    if "flight" in df.columns and "flightkey" not in df.columns:
        rename_map["flight"] = "flightkey"
    if "flightkey" not in df.columns:
        # maybe header uses FlightKey or FlightKey variations already normalized above
        pass

    if "originairport" in df.columns and "originairportkey" not in df.columns:
        rename_map["originairport"] = "originairportkey"
    if "origin" in df.columns and "originairportkey" not in df.columns:
        rename_map["origin"] = "originairportkey"

    if "destinationairport" in df.columns and "destinationairportkey" not in df.columns:
        rename_map["destinationairport"] = "destinationairportkey"
    if "destination" in df.columns and "destinationairportkey" not in df.columns:
        rename_map["destination"] = "destinationairportkey"

    if "aircraft_type" in df.columns and "aircrafttype" not in df.columns:
        rename_map["aircraft_type"] = "aircrafttype"
    if "aircraft" in df.columns and "aircrafttype" not in df.columns:
        rename_map["aircraft"] = "aircrafttype"

    if rename_map:
        df = df.rename(columns=rename_map)

    # trim string cols
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
            "flightkey": r.get("flightkey"),
            "originairportkey": r.get("originairportkey"),
            "destinationairportkey": r.get("destinationairportkey"),
            "aircrafttype": r.get("aircrafttype"),
            "rawjson": r
        })
    return cleaned_rows, raw_rows

# upsert to cleaned_flights table (used when ingesting directly)
def _upsert_cleaned_flight_row(row: Dict[str, Any], upload_id: int) -> None:
    payload = {
        "flightkey": row.get("flightkey"),
        "originairportkey": row.get("originairportkey"),
        "destinationairportkey": row.get("destinationairportkey"),
        "aircrafttype": row.get("aircrafttype"),
        "rawjson": row.get("rawjson") if "rawjson" in row else row,
        "upload_id": upload_id,
        "processed": False,
        "insertedat": datetime.utcnow().isoformat(),
        "error_count": 0,
        "last_error": None
    }
    # upsert on flightkey (your dimflight uses flightkey as business key)
    res = sb.table("cleaned_flights").upsert(payload, on_conflict="flightkey").execute()
    if isinstance(res, dict) and res.get("error"):
        raise Exception(res.get("error"))
    if hasattr(res, "error") and res.error:
        raise Exception(str(res.error))

def process_flights_upload(upload_id: int, raw: Optional[Dict[str, Any]], run_id: int = None) -> Dict[str, int]:
    """
    ETL entrypoint used by dispatcher/CLI.
    Consumes staging_raw.raw and upserts cleaned_flights (via supabase client).
    """
    rows = []
    if not raw:
        return {"processed": 0, "errors": 0}

    if isinstance(raw, dict) and "rows" in raw and isinstance(raw["rows"], list):
        rows = raw["rows"]
    elif isinstance(raw, dict) and "raw_rows" in raw and isinstance(raw["raw_rows"], list):
        rows = [r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r for r in raw["raw_rows"]]
    elif isinstance(raw, list):
        rows = raw
    else:
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
            fk = rec.get("flightkey") or rec.get("flight_number") or rec.get("flight")
            if not fk:
                errors += 1
                sb.table("import_errors").insert({
                    "sourcetable": "cleaned_flights",
                    "sourceid": None,
                    "raw": rec,
                    "errormessage": "missing flightkey",
                    "createdat": datetime.utcnow().isoformat()
                }).execute()
                continue

            normalized = {
                "flightkey": fk,
                "originairportkey": rec.get("originairportkey") or rec.get("origin") or rec.get("originairport"),
                "destinationairportkey": rec.get("destinationairportkey") or rec.get("destination") or rec.get("destinationairport"),
                "aircrafttype": rec.get("aircrafttype") or rec.get("aircraft_type") or rec.get("aircraft"),
                "rawjson": rec
            }
            _upsert_cleaned_flight_row(normalized, upload_id)
            processed += 1
        except Exception as e:
            errors += 1
            try:
                sb.table("import_errors").insert({
                    "sourcetable": "cleaned_flights",
                    "sourceid": None,
                    "raw": r,
                    "errormessage": str(e),
                    "createdat": datetime.utcnow().isoformat()
                }).execute()
            except Exception:
                pass

    return {"processed": processed, "errors": errors}

# optional quick test when run directly
if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "sample_flights.csv"
    cleaned, raw = clean_file(p)
    print("Cleaned rows:", len(cleaned))
    print(cleaned[:3])
