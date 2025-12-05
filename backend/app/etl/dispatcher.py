# backend/app/etl/dispatcher.py
from typing import Dict, Callable, Any
from app.etl.airlines_etl import process_airlines_upload
from app.etl.passengers_etl import process_passengers_upload
from app.etl.flights_etl import process_flights_upload
from app.etl.travelagency_etl import process_travelagency_upload
from app.etl.airports_etl import process_airports_upload   # <-- ADD THIS IMPORT

ETL_HANDLERS: Dict[str, Callable[[int, dict, int], Any]] = {
    "airline": process_airlines_upload,
    "airlines": process_airlines_upload,

    "passenger": process_passengers_upload,
    "passengers": process_passengers_upload,

    "flight": process_flights_upload,
    "flights": process_flights_upload,

    "travelagency": process_travelagency_upload,
    "travel_agency": process_travelagency_upload,

    # ✅ FIX: add airport mappings
    "airport": process_airports_upload,
    "airports": process_airports_upload,
}

def dispatch_etl(upload_id: int, entity: str, raw: dict, run_id: int = None):
    handler = ETL_HANDLERS.get((entity or "").lower())
    if not handler:
        raise ValueError(f"No ETL handler for entity '{entity}'")
    return handler(upload_id, raw, run_id)
