# backend/app/etl/corporatesales_etl.py
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Dict, Any
import pandas as pd
import io

def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, engine="python")
    except Exception:
        return pd.read_csv(path, engine="python", encoding="latin-1")

def clean_file(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Minimal corporatesales ETL stub.
    Converts incoming CSV/DOCX into:
      - cleaned rows with keys: invoiceid, corporate_id, corporate_name, item, qty, unitprice, total, currency, saledate, rawjson
      - raw rows as {"rawjson": {...}}
    This will be compatible with your upload pipeline while you flesh it out.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    ext = p.suffix.lower()
    if ext == ".docx":
        # simple fallback: treat as text lines (your full ETL may parse docx tables)
        text = p.read_text(encoding="utf-8", errors="ignore")
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            raise RuntimeError("DOCX contained no usable lines")
        csv_buf = io.StringIO("\n".join(lines))
        df = pd.read_csv(csv_buf, engine="python")
    else:
        df = _read_csv(p)

    # normalize columns to snake-like names
    df.columns = (
        df.columns.str.strip()
        .str.replace(r"([a-z0-9])([A-Z])", r"\1_\2", regex=True)
        .str.replace(r"[ \-]+", "_", regex=True)
        .str.lower()
    )

    # safe coercions / selects: keep common columns if present
    def safe_col(key):
        return key if key in df.columns else None

    cleaned = []
    raw_rows = []

    for _, row in df.fillna("").iterrows():
        r = row.to_dict()
        # simple normalization: prefer common variants
        invoiceid = r.get("invoiceid") or r.get("invoice_id") or r.get("transactionid") or r.get("transaction_id") or None
        corporate_id = r.get("corporate_id") or r.get("corp_id") or None
        corporate_name = r.get("corporate_name") or r.get("company") or r.get("corporate") or None
        item = r.get("item") or r.get("description") or None
        qty = r.get("qty") or r.get("quantity") or None
        unitprice = r.get("unitprice") or r.get("price") or None
        total = r.get("total") or r.get("amount") or None
        currency = r.get("currency") or None
        saledate = r.get("saledate") or r.get("date") or None

        raw_rows.append({"rawjson": r})
        cleaned.append({
            "invoiceid": invoiceid,
            "transactionid": invoiceid,  # convenience: set transactionid = invoiceid if present
            "corporate_id": corporate_id,
            "corporate_name": corporate_name,
            "item": item,
            "qty": qty,
            "unitprice": unitprice,
            "total": total,
            "currency": currency,
            "saledate": saledate,
            "rawjson": r,
        })

    return cleaned, raw_rows


if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "sample_corporate.csv"
    cleaned, raw = clean_file(p)
    print("Cleaned:", len(cleaned))
