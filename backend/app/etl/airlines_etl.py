# backend/app/etl/airlines_etl.py
"""
airlines_etl.clean_file(path) -> (cleaned_rows, raw_rows)

- Accepts a path to a .csv or .docx file.
- For .docx: if the document contains tables, uses the first table as tabular data;
  otherwise attempts to parse paragraph lines as CSV rows.
- For .csv: uses pandas.read_csv
- Normalizes column names to snake_case, lower-case, underscores.
- Returns:
    cleaned_rows: list[dict]  -- cleaned records (airline_key, airline_name, alliance, raw_json, inserted_at optional)
    raw_rows: list[dict]      -- raw json-like dicts for staging_raw
"""
from __future__ import annotations
import os
from pathlib import Path
import io
import json
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Any

# optional dependency for docx reading
try:
    from docx import Document
except Exception:
    Document = None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize column names: strip, replace spaces/dashes, camel->snake
    cols = (
        df.columns.str.strip()
        .str.replace(r"([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.replace(r"[ \-]+", "_", regex=True)
        .str.lower()
    )
    df.columns = cols
    return df


def _df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    # ensure object columns are stripped of whitespace
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})
    # convert pandas NaN to None
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def _read_docx_table(path: Path) -> pd.DataFrame:
    if Document is None:
        raise RuntimeError("python-docx is not installed. Install python-docx in your backend venv.")
    doc = Document(str(path))
    # prefer first table if it exists
    if doc.tables:
        table = doc.tables[0]
        rows = []
        for r_i, row in enumerate(table.rows):
            cells = [c.text.strip() for c in row.cells]
            rows.append(cells)
        # first row as header
        if len(rows) >= 2:
            header = rows[0]
            data = rows[1:]
            # build CSV-like string then load with pandas
            csv_buf = io.StringIO()
            csv_buf.write(",".join(header) + "\n")
            for r in data:
                # escape commas in cells by quoting
                row_escaped = []
                for cell in r:
                    # double-quote any cell containing comma or quote
                    if '"' in cell:
                        cell = cell.replace('"', '""')
                    if "," in cell or '"' in cell:
                        row_escaped.append(f'"{cell}"')
                    else:
                        row_escaped.append(cell)
                csv_buf.write(",".join(row_escaped) + "\n")
            csv_buf.seek(0)
            df = pd.read_csv(csv_buf, engine="python")
            return df
    # fallback: try to parse paragraphs as CSV lines
    doc_text_lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
    if not doc_text_lines:
        raise RuntimeError("DOCX contained no tables or paragraphs with data.")
    csv_buf = io.StringIO("\n".join(doc_text_lines))
    df = pd.read_csv(csv_buf, engine="python")
    return df


def _read_csv_file(path: Path) -> pd.DataFrame:
    # try a few common encodings if needed
    try:
        return pd.read_csv(path, engine="python")
    except Exception:
        # fallback reading as latin-1
        return pd.read_csv(path, engine="python", encoding="latin-1")


def clean_file(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Main entrypoint expected by backend.main: accepts a path to uploaded file.
    Returns (cleaned_rows, raw_rows), both lists of dicts.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    ext = p.suffix.lower()
    df: pd.DataFrame

    # Read file into DataFrame
    if ext == ".docx":
        df = _read_docx_table(p)
    elif ext in (".csv", ".txt"):
        df = _read_csv_file(p)
    else:
        # try to read as CSV anyway
        df = _read_csv_file(p)

    # Normalize columns
    df = _normalize_columns(df)

    # If there is no column that looks like airline_key but there is some PassengerKey etc, keep everything
    # We try to standardize the expected fields: airline_key, airline_name, alliance
    # If there's a single unnamed column with all data, try splitting? (out of scope here)
    expected_cols = {"airline_key", "airline_name", "alliance"}
    available = set(df.columns)

    # Best-effort: if 'airline' present, rename
    if "airline" in df.columns and "airline_name" not in df.columns:
        df = df.rename(columns={"airline": "airline_name"})

    # Basic trimming for string cols
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip().replace({"nan": None, "None": None})

    # Standardize airline_key if present
    if "airline_key" in df.columns:
        df["airline_key"] = df["airline_key"].astype(str).str.strip()
        # normalize common empty indicators
        df["airline_key"] = df["airline_key"].replace({"nan": None, "": None, "None": None})
        # uppercase keys
        df["airline_key"] = df["airline_key"].where(pd.notnull(df["airline_key"]), None)
        df.loc[df["airline_key"].notnull(), "airline_key"] = df.loc[df["airline_key"].notnull(), "airline_key"].str.upper()

    # Normalise airline_name capitalization if present
    if "airline_name" in df.columns:
        df["airline_name"] = df["airline_name"].astype(str).apply(lambda s: s.strip().title() if s and s.strip().lower() not in ("nan", "none") else None)

    # alliance fill / normalize
    if "alliance" in df.columns:
        df["alliance"] = df["alliance"].replace({np.nan: None, "nan": None, "NAN": None, "": None})
        df["alliance"] = df["alliance"].astype(object).where(pd.notnull(df["alliance"]), None)
        # if all None, keep None; else keep provided string
        df.loc[df["alliance"].notnull(), "alliance"] = df.loc[df["alliance"].notnull(), "alliance"].astype(str).str.strip()

    # Drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    removed = before - len(df)

    # Build raw_rows and cleaned_rows
    records = _df_to_records(df)

    cleaned_rows: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []

    for r in records:
        # ensure raw_json contains original values
        raw_rows.append({"raw_json": r})
        # Build cleaned record with selected fields (safe defaults)
        cleaned = {
            "airline_key": r.get("airline_key"),
            "airline_name": r.get("airline_name"),
            "alliance": r.get("alliance"),
            "raw_json": r  # keep original row for traceability
        }
        cleaned_rows.append(cleaned)

    return cleaned_rows, raw_rows


# If run as script, simple CLI test
if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "sample_airlines.csv"
    print("Reading:", p)
    cleaned, raw = clean_file(p)
    print("Cleaned rows:", len(cleaned))
    print(cleaned[:3])