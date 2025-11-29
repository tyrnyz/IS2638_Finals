# backend/main.py
import os
import tempfile
import shutil
import time
from typing import List, Dict, Tuple, Any
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# Load .env (optional here, supabase_client also loads backend/.env)
load_dotenv()

# ---- package imports matching your repository layout ----
# ETL modules live at backend/app/etl/
from backend.app.etl import airlines_etl
# supabase client lives at backend/app/services/
from backend.app.services.supabase_client import sb

# FastAPI + CORS
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="ETL Upload API (IS2638)")

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# dataset -> cleaned table, rpc, module
# NOTE: rpc now points to a single-RPC that processes ALL rows (no batching).
DATASET_MAP = {
    "airline": {"cleaned_table": "cleaned_airlines", "rpc": "process_cleaned_airlines", "etl_module": airlines_etl},
    "airlines": {"cleaned_table": "cleaned_airlines", "rpc": "process_cleaned_airlines", "etl_module": airlines_etl},
    # passenger/flight removed while you iterate (add later)
}

# config (env overrides)
BATCH_INSERT_SIZE = int(os.getenv("BATCH_INSERT_SIZE", "200"))
# RPC_BATCH_LIMIT no longer used for processing; kept for compatibility
RPC_BATCH_LIMIT = int(os.getenv("RPC_BATCH_LIMIT", "500"))
RPC_SLEEP_SEC = float(os.getenv("RPC_SLEEP_SEC", "0.05"))
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", str(10 * 1024 * 1024)))  # default 10MB
STORE_UPLOADS = os.getenv("STORE_UPLOADS", "false").lower() in ("1", "true", "yes")

# -----------------------
# Helpers
# -----------------------
def batch_insert(table_name: str, records: List[Dict[str, Any]], batch_size: int = BATCH_INSERT_SIZE) -> int:
    if not records:
        return 0
    inserted = 0
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        res = sb.table(table_name).insert(chunk).execute()
        # supabase-py may return a dict or object with .error/.data
        if isinstance(res, dict) and res.get("error"):
            raise RuntimeError(f"Error inserting into {table_name}: {res.get('error')}")
        if hasattr(res, "error") and res.error:
            raise RuntimeError(f"Error inserting into {table_name}: {res.error}")
        inserted += len(chunk)
    return inserted

def parse_rpc_count(res) -> int:
    """
    Parse a typical supabase rpc response to extract a single integer count.
    Works with different response shapes (res.data, dict, etc).
    """
    try:
        if hasattr(res, "data"):
            d = res.data
            if isinstance(d, list) and d:
                first = d[0]
                if isinstance(first, dict):
                    # find first int value
                    for v in first.values():
                        if isinstance(v, int):
                            return v
                if isinstance(first, int):
                    return first
            if isinstance(d, int):
                return d
    except Exception:
        pass
    if isinstance(res, dict):
        if "data" in res:
            d = res["data"]
            if isinstance(d, list) and d:
                first = d[0]
                if isinstance(first, dict):
                    for v in first.values():
                        if isinstance(v, int):
                            return v
                if isinstance(first, int):
                    return first
            if isinstance(d, int):
                return d
        for k in ("count", "rows_affected", "result"):
            if k in res and isinstance(res[k], int):
                return res[k]
    return 0

def call_rpc_once(rpc_name: str) -> int:
    """
    Call a single RPC that is expected to process all unprocessed rows and return an integer count.
    This replaces a batch loop.
    """
    res = sb.rpc(rpc_name, {}).execute()
    return parse_rpc_count(res)

def insert_etl_run(job_name: str, status: str, note: str = None) -> int:
    try:
        rec = {"job_name": job_name, "status": status, "note": note}
        res = sb.table("etl_runs").insert(rec).execute()
        if isinstance(res, dict) and res.get("error"):
            print("Warning: could not insert etl_runs:", res.get("error"))
            return -1
        if hasattr(res, "data") and isinstance(res.data, list) and res.data:
            return res.data[0].get("id", -1)
    except Exception as e:
        print("Warning: insert_etl_run failed:", e)
    return -1

# -----------------------
# Upload endpoint
# -----------------------
@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), dataset: str = Form(...)):
    dataset_key = dataset.lower().strip()
    if dataset_key not in DATASET_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported dataset: {dataset}")

    cfg = DATASET_MAP[dataset_key]
    cleaned_table = cfg["cleaned_table"]
    rpc_name = cfg["rpc"]
    etl_module = cfg["etl_module"]

    # save temporary file
    suffix = "." + (file.filename.split(".")[-1] if "." in file.filename else "tmp")
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    run_id = insert_etl_run(f"upload_{dataset_key}", "started", note=file.filename)

    try:
        # server-side validation
        _, ext = os.path.splitext(file.filename.lower())
        allowed = {".csv", ".docx"}
        if ext not in allowed:
            raise HTTPException(status_code=400, detail=f"Unsupported file extension: {ext}. Allowed: {', '.join(allowed)}")
        try:
            size = os.path.getsize(tmp_path)
            if size > MAX_FILE_BYTES:
                raise HTTPException(status_code=413, detail=f"File too large ({size} bytes). Max allowed is {MAX_FILE_BYTES} bytes.")
        except OSError:
            pass

        # optional: store original upload to Supabase Storage (bucket 'uploads' must exist)
        if STORE_UPLOADS:
            try:
                dest_path = f"uploads/{int(time.time())}_{os.path.basename(file.filename)}"
                with open(tmp_path, "rb") as fh:
                    data_bytes = fh.read()
                upload_res = sb.storage.from_("uploads").upload(dest_path, data_bytes, {"cacheControl":"3600"})
                if isinstance(upload_res, dict) and upload_res.get("error"):
                    print("Warning: storage upload error:", upload_res.get("error"))
                else:
                    print("Saved original upload to storage:", dest_path)
            except Exception as e:
                print("Warning: storing original upload failed:", str(e))

        # 1) run ETL module -> cleaned_rows, raw_rows
        cleaned_rows, raw_rows = etl_module.clean_file(tmp_path)
        if raw_rows is None:
            raw_rows = [r.get("raw_json", r) for r in cleaned_rows]

        # defensive: ensure rows are list-of-dicts
        if not isinstance(cleaned_rows, list) or not all(isinstance(x, dict) for x in cleaned_rows):
            raise RuntimeError("ETL module returned invalid cleaned_rows type; expected list[dict].")
        if not isinstance(raw_rows, list) or not all(isinstance(x, dict) for x in raw_rows):
            raw_rows = [x if isinstance(x, dict) else {"value": x} for x in raw_rows]

        # 2) insert staging_raw for traceability
        staging_records = [{"entity": dataset_key, "raw": r} for r in raw_rows]
        staged_count = batch_insert("staging_raw", staging_records)

        # 3) insert cleaned rows into cleaned_table
        cleaned_records = []
        for r in cleaned_rows:
            rec = dict(r)
            rec.setdefault("raw_json", r.get("raw_json", r))
            cleaned_records.append(rec)
        cleaned_count = batch_insert(cleaned_table, cleaned_records)

        # 4) call single RPC to upsert into dims (process ALL rows at once)
        processed_count = call_rpc_once(rpc_name)

        # 5) update etl_runs
        try:
            sb.table("etl_runs").update({"status": "success", "finished_at": "now()"}).eq("id", run_id).execute()
        except Exception:
            pass

        return JSONResponse({
            "status": "ok",
            "dataset": dataset_key,
            "filename": file.filename,
            "staged": staged_count,
            "cleaned_inserted": cleaned_count,
            "processed_into_dims": processed_count
        })
    except Exception as e:
        try:
            sb.table("import_errors").insert({
                "source_table": cleaned_table,
                "source_id": None,
                "raw": {"filename": file.filename, "error": str(e)},
                "error_message": "ETL pipeline exception"
            }).execute()
        except Exception:
            pass
        try:
            sb.table("etl_runs").update({"status": "failed", "note": str(e), "finished_at": "now()"}).eq("id", run_id).execute()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass