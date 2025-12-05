# backend/app/etl/airports_etl.py
"""
Airports ETL: clean_file(path) and process_airports_upload(upload_id, raw, run_id=None)

Canonical fields only:
 - airportkey (business key)
 - airportname
 - city
 - country
 - rawjson

Minimal dimairport mapping: airportkey, airportname, city, country, createdat

clean_file(path) -> (cleaned_rows, raw_rows)
 - cleaned_rows: list[{"airportkey","airportname","city","country","rawjson"}]
 - raw_rows: list[{"rawjson": {...}}]

This implementation is tolerant for parsing (DOCX/CSV) but intentionally
does NOT include legacy handling for 'iata', 'icao', 'lat', 'lon', etc.
"""
from __future__ import annotations
from pathlib import Path
import zipfile, io, csv, re, json
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

# NOTE: replace this import with your actual supabase client instance
from ..services.supabase_client import sb
from .. import parsers  # tolerant parser / parse warnings (if present)

# -------------------- Helpers: DOCX/CSV extraction --------------------
def _extract_docx_lines(path: Path) -> List[str]:
    """
    Extract text lines from a .docx file by reading word/document.xml.
    If that fails, try a plain-text decode fallback.
    """
    lines: List[str] = []
    try:
        with zipfile.ZipFile(path, 'r') as z:
            xml = z.read('word/document.xml').decode('utf-8', errors='ignore')
            xml = re.sub(r'</w:p>', '\n', xml)
            parts = re.findall(r'<w:t[^>]*>(.*?)</w:t>', xml, flags=re.DOTALL)
            if parts:
                raw = "".join(parts)
                lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
                if lines:
                    return lines
    except Exception:
        pass

    try:
        raw = path.read_bytes().decode('utf-8', errors='ignore')
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        return lines
    except Exception:
        return []

def _parse_lines_to_rows(lines: List[str]) -> List[List[str]]:
    """
    Parse each textual line into CSV fields using csv.reader (handles quotes).
    If parsing fails for a line, fall back to manual comma-split.
    """
    rows = []
    for ln in lines:
        if not ln or not ln.strip():
            continue
        try:
            reader = csv.reader([ln])
            row = next(reader)
            rows.append([c if c != "" else None for c in row])
        except Exception:
            parts = [p.strip().strip('"').strip("'") for p in ln.split(",")]
            rows.append([p if p != "" else None for p in parts])
    return rows

def _rows_to_dataframe(rows: List[List[str]]) -> "pd.DataFrame":
    """
    Convert parsed rows into a DataFrame.
    If the first row looks like a header (contains canonical tokens)
    use it as header. Otherwise assume four columns:
    airportkey, airportname, city, country.
    """
    import pandas as pd
    if not rows:
        return pd.DataFrame()
    first = [str(c).lower() if c is not None else "" for c in rows[0]]
    header_keywords = ("airportkey", "airport", "airport_name", "airportname", "name", "city", "country")
    is_header = any(any(k in cell for k in header_keywords) for cell in first)
    if is_header:
        header = [str(c).strip().lower().replace(" ", "_") for c in rows[0]]
        # Do NOT map legacy tokens; require canonical names in downstream schema
        data = rows[1:]
        norm = []
        for r in data:
            r2 = list(r)
            while len(r2) < len(header):
                r2.append(None)
            norm.append(r2[:len(header)])
        df = pd.DataFrame(norm, columns=header)
    else:
        cols = ["airportkey", "airportname", "city", "country"]
        norm = []
        for r in rows:
            r2 = list(r)
            if len(r2) > 4:
                if len(r2[0] or "") <= 5:
                    code = r2[0]
                    city = r2[-2] if len(r2) >= 3 else None
                    country = r2[-1] if len(r2) >= 2 else None
                    name = ", ".join([p for p in r2[1:-2] if p])
                    r2 = [code, name, city, country]
                else:
                    r2 = [r2[0], ", ".join(r2[1:-2]) if len(r2) > 2 else None,
                          r2[-2] if len(r2) >= 3 else None, r2[-1] if len(r2) >= 2 else None]
            while len(r2) < 4:
                r2.append(None)
            norm.append(r2[:4])
        df = pd.DataFrame(norm, columns=cols)
    return df

# -------------------- Cleaning / Normalization --------------------
def _clean_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.strip('"').strip("'")
    s = re.sub(r'\s+', ' ', s)
    if s.lower() in ("nan", "none", "null"):
        return None
    return s

def _normalize_country(c: Optional[str]) -> Optional[str]:
    if not c:
        return None
    s = str(c).strip().replace(".", "").lower()
    mappings = {
        "us": "United States", "usa": "United States", "u s a": "United States", "u s": "United States",
        "uk": "United Kingdom", "u k": "United Kingdom"
    }
    key = s.replace(",", "").strip()
    return mappings.get(key, c.strip())

def _df_to_cleaned_records(df: "pd.DataFrame") -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    import pandas as pd
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    for col in ("airportkey", "airportname", "city", "country"):
        if col not in df.columns:
            df[col] = None
    for col in ("airportkey", "airportname", "city", "country"):
        df[col] = df[col].apply(_clean_text)
    # Uppercase airportkey if present
    df["airportkey"] = df["airportkey"].apply(lambda x: x.upper() if isinstance(x, str) else x)
    df["country"] = df["country"].apply(_normalize_country)
    # drop rows that have neither key nor name
    df = df[~(df["airportkey"].isnull() & df["airportname"].isnull())].copy()
    # remove exact duplicates (canonical fields)
    df["__dedup_key"] = (df["airportkey"].fillna("") + "|" +
                         df["airportname"].fillna("").str.lower() + "|" +
                         df["city"].fillna("").str.lower() + "|" +
                         df["country"].fillna("").str.lower())
    df = df.drop_duplicates(subset="__dedup_key")
    df = df.drop(columns="__dedup_key")
    df = df.reset_index(drop=True)

    cleaned_rows = []
    raw_rows = []
    for _, r in df.iterrows():
        raw = {c: (r.get(c) if c in r.index else None) for c in r.index}
        raw_rows.append({"rawjson": raw})
        cleaned_rows.append({
            "airportkey": raw.get("airportkey"),
            "airportname": raw.get("airportname"),
            "city": raw.get("city"),
            "country": raw.get("country"),
            "rawjson": raw
        })
    return cleaned_rows, raw_rows

# -------------------- Public API --------------------
def clean_file(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (cleaned_rows, raw_rows) as described above.
    Works for .docx files (word) and plain CSVs.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    lines = _extract_docx_lines(p)
    if not lines:
        try:
            raw = p.read_text(encoding="utf-8", errors="ignore")
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        except Exception:
            lines = []

    rows = _parse_lines_to_rows(lines)
    df = _rows_to_dataframe(rows)
    cleaned_rows, raw_rows = _df_to_cleaned_records(df)
    return cleaned_rows, raw_rows

# -------------------- DB upsert helpers (minimal dimairport) --------------------
def _upsert_dimairport_by_key(payload: Dict[str, Any]) -> None:
    """
    Upsert into dimairport using 'airportkey' unique constraint.
    Writes only canonical minimal columns.
    """
    if not payload.get("airportkey"):
        raise ValueError("missing airportkey for upsert")
    mapped = {
        "airportkey": payload.get("airportkey"),
        "airportname": payload.get("airportname"),
        "city": payload.get("city"),
        "country": payload.get("country"),
        "createdat": payload.get("createdat", datetime.utcnow().isoformat())
    }
    res = sb.table("dimairport").upsert(mapped, on_conflict="airportkey").execute()
    if isinstance(res, dict) and res.get("error"):
        raise Exception(res.get("error"))
    if hasattr(res, "error") and res.error:
        raise Exception(str(res.error))

def _insert_dimairport(payload: Dict[str, Any]) -> None:
    mapped = {
        "airportkey": payload.get("airportkey"),
        "airportname": payload.get("airportname"),
        "city": payload.get("city"),
        "country": payload.get("country"),
        "createdat": payload.get("createdat", datetime.utcnow().isoformat())
    }
    res = sb.table("dimairport").insert(mapped).execute()
    if isinstance(res, dict) and res.get("error"):
        raise Exception(res.get("error"))
    if hasattr(res, "error") and res.error:
        raise Exception(str(res.error))

def _upsert_airport_row(row: Dict[str, Any], upload_id: int) -> None:
    """
    Row shape expected from clean_file entries.
    Writes canonical minimal fields to dimairport, and inserts/upserts into cleaned_airports.
    """
    airportkey = row.get("airportkey") or None
    payload = {
        "airportkey": (airportkey.strip().upper() if isinstance(airportkey, str) else airportkey),
        "airportname": row.get("airportname"),
        "city": row.get("city"),
        "country": row.get("country"),
        "rawjson": row.get("rawjson"),
        "createdat": datetime.utcnow().isoformat()
    }

    # Upsert to dimairport
    if payload.get("airportkey"):
        try:
            _upsert_dimairport_by_key(payload)
        except Exception:
            _insert_dimairport(payload)
    else:
        _insert_dimairport(payload)

    # Upsert cleaned_airports (only canonical columns)
    cleaned_payload = {
        "id": row.get("id"),
        "airportkey": payload.get("airportkey"),
        "airportname": payload.get("airportname"),
        "city": payload.get("city"),
        "country": payload.get("country"),
        "rawjson": payload.get("rawjson"),
        "upload_id": upload_id,
        "processed": True,
        "insertedat": datetime.utcnow().isoformat()
    }

    try:
        if row.get("id"):
            res = sb.table("cleaned_airports").upsert(cleaned_payload, on_conflict="id").execute()
        else:
            insert_payload = {k: v for k, v in cleaned_payload.items() if k != "id"}
            res = sb.table("cleaned_airports").insert(insert_payload).execute()
        if isinstance(res, dict) and res.get("error"):
            raise Exception(res.get("error"))
        if hasattr(res, "error") and res.error:
            raise Exception(str(res.error))
    except Exception:
        raise

# -------------------- process function --------------------
def process_airports_upload(upload_id: int, raw: Optional[Dict[str, Any]], run_id: int = None) -> Dict[str, int]:
    """
    Accepts staging_raw.raw shapes: dict with 'rows' or 'raw_rows', or a list.
    Upserts each row into dimairport and writes/updates cleaned_airports.
    Returns {"processed": n, "errors": m}
    """
    rows: List[Dict[str, Any]] = []
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

    parse_warnings = getattr(parsers, "LAST_CSV_PARSE_ERRORS", None)
    if parse_warnings:
        try:
            sb.table("import_errors").insert({
                "sourcetable": "staging_raw",
                "sourceid": upload_id,
                "raw": {"upload_id": upload_id, "parse_warnings": parse_warnings},
                "errormessage": "csv_parse_warnings",
                "createdat": datetime.utcnow().isoformat()
            }).execute()
        except Exception:
            pass

    for r in rows:
        try:
            rec = r.get("rawjson") if isinstance(r, dict) and "rawjson" in r else r
            normalized = {
                "airportkey": rec.get("airportkey") if isinstance(rec, dict) else None,
                "airportname": rec.get("airportname") or rec.get("airport_name") or rec.get("name"),
                "city": rec.get("city"),
                "country": rec.get("country"),
                "rawjson": rec,
                "id": rec.get("id") if isinstance(rec, dict) else None
            }

            # require at least one identifier or name (canonical)
            if not (normalized.get("airportkey") or normalized.get("airportname")):
                errors += 1
                try:
                    sb.table("import_errors").insert({
                        "sourcetable": "staging_raw",
                        "sourceid": upload_id,
                        "raw": normalized.get("rawjson"),
                        "errormessage": "missing identifiers (airportkey/airportname)",
                        "createdat": datetime.utcnow().isoformat()
                    }).execute()
                except Exception:
                    pass
                continue

            _upsert_airport_row(normalized, upload_id)
            processed += 1
        except Exception as e:
            errors += 1
            try:
                sb.table("import_errors").insert({
                    "sourcetable": "staging_raw",
                    "sourceid": upload_id,
                    "raw": r if isinstance(r, dict) else {"row": r},
                    "errormessage": str(e),
                    "createdat": datetime.utcnow().isoformat()
                }).execute()
            except Exception:
                pass

    return {"processed": processed, "errors": errors}

# -------------------- Quick local test (prints cleaned count) --------------------
if __name__ == "__main__":
    import sys, csv
    p = sys.argv[1] if len(sys.argv) > 1 else "airports.csv.docx"
    try:
        cleaned, raw = clean_file(p)
        print("Cleaned rows:", len(cleaned))
        out = Path("cleaned_airports_preview.csv")
        with out.open("w", newline='', encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["airportkey", "airportname", "city", "country"])
            for r in cleaned:
                w.writerow([r.get("airportkey"), r.get("airportname"), r.get("city"), r.get("country")])
        print("Saved preview to:", out.resolve())
    except Exception as exc:
        print("Error during local test:", exc)
