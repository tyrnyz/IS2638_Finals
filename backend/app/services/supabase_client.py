# backend/app/services/supabase_client.py
import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Determine path to backend/.env relative to this file
# this file is expected at backend/app/services/supabase_client.py
HERE = Path(__file__).resolve()
# parents: 0=services, 1=app, 2=backend
BACKEND_DIR = HERE.parents[2]
DOTENV_PATH = BACKEND_DIR / ".env"

# Load the .env at backend/.env (falls back to default if not found)
if DOTENV_PATH.exists():
    load_dotenv(dotenv_path=str(DOTENV_PATH))
else:
    # fallback to any .env found in cwd or system environment
    load_dotenv()

# Now read environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # server-only key

if not SUPABASE_URL or not SUPABASE_KEY or "REPLACE_WITH" in (SUPABASE_KEY or ""):
    # helpful error message that shows where we looked
    raise RuntimeError(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in backend/.env.\n"
        f"Tried to load: {DOTENV_PATH}\n"
        "Open backend/.env and paste your SUPABASE service role key into SUPABASE_SERVICE_ROLE_KEY."
    )

sb = create_client(SUPABASE_URL, SUPABASE_KEY)