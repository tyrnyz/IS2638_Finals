# backend/app/parsers.py
import csv
import io
from typing import Dict, List, Optional, Any, Tuple

# Public module-level variable: after calling parse_csv_bytes_to_rows(),
# any non-fatal parse warnings will be available here as a list of strings.
LAST_CSV_PARSE_ERRORS: List[str] = []


def _parse_csv_bytes_to_rows_with_errors(content: bytes, *, strict: bool = False) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Tolerant CSV parser that returns (rows, errors).

    Behavior:
    - Detects delimiter using csv.Sniffer (tries comma/semicolon/tab/pipe).
    - Reads header, normalizes header strings by stripping whitespace.
    - For each row:
        - If row has more fields than header, merge extras into the last field.
        - If row has fewer fields, pad missing fields with empty strings.
    - If strict=True, a malformed row will raise ValueError.
    - Returns parsed rows (list of dict) and parse warnings/errors (list of strings).
    """
    text = content.decode("utf-8", errors="replace")
    sample = text[:8192]
    delimiter = ","
    dialect = None

    # sniff delimiter
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
        delimiter = dialect.delimiter
    except Exception:
        # fallback to comma-like dialect
        dialect = csv.get_dialect("excel")
        dialect.delimiter = ","
        delimiter = ","

    stream = io.StringIO(text)
    reader = csv.reader(stream, dialect)

    errors: List[str] = []
    rows_out: List[Dict[str, Any]] = []

    try:
        header = next(reader)
    except StopIteration:
        return [], ["empty file"]

    headers = [h.strip() for h in header]
    ncols = len(headers)
    line_no = 1  # header consumed

    for raw_row in reader:
        line_no += 1

        # handle BOM or weird empty rows
        if len(raw_row) == 1 and raw_row[0] == "":
            # empty row, skip
            continue

        row = raw_row

        if len(row) == ncols:
            pass
        elif len(row) > ncols:
            # merge extras into the last column
            merged_last = delimiter.join(row[ncols - 1 :])
            row = row[: ncols - 1] + [merged_last]
            errors.append(f"merged_extra_fields at line {line_no}: expected {ncols} saw {len(raw_row)} (merged extras into last field)")
        else:
            # fewer fields -> pad
            pad_len = ncols - len(row)
            row = row + ([""] * pad_len)
            errors.append(f"padded_missing_fields at line {line_no}: expected {ncols} saw {len(raw_row)} (padded with empty strings)")

        # build dict mapping header -> value
        record: Dict[str, Any] = {headers[i]: row[i].strip() for i in range(ncols)}
        rows_out.append(record)

        if strict and errors:
            raise ValueError(errors[-1])

    return rows_out, errors


def parse_csv_bytes_to_rows(content: bytes) -> List[Dict[str, Any]]:
    """
    Backwards-compatible parser used by ETL modules.

    Returns only the list of parsed rows (list[dict]).
    Non-fatal parse warnings are stored in LAST_CSV_PARSE_ERRORS for callers to inspect.
    """
    global LAST_CSV_PARSE_ERRORS
    LAST_CSV_PARSE_ERRORS = []
    rows, errors = _parse_csv_bytes_to_rows_with_errors(content, strict=False)
    LAST_CSV_PARSE_ERRORS = errors
    return rows


# ----------------------------
# Entity detection helpers
# ----------------------------
def detect_entity_from_headers(headers: List[str]) -> Optional[str]:
    # normalize headers
    h = {x.lower().strip() for x in headers if x}

    # PASSENGERS
    if {"passenger_id", "name"}.issubset(h) or {"first_name", "last_name"}.issubset(h):
        return "passenger"

    # AIRLINES
    if {
        "iata", "icao", "airline_name", "airlinename", "airlinekey",
        "callsign", "country"
    }.intersection(h):
        return "airline"

    # AIRPORTS
    if {
        "airport_id", "airportname", "airport_name",
        "city", "country", "iata_code", "icao_code", "latitude", "longitude", "iata", "icao"
    }.intersection(h):
        return "airport"

    # FLIGHTS
    if {
        "flight_no", "flight_number", "departure", "arrival",
        "origin", "destination", "scheduled_time", "status", "airline_id", "flightkey"
    }.intersection(h):
        return "flight"

    # TRAVEL AGENCY SALES
    if {
        "booking_id", "sale_amount", "agency_id", "agency_name", "sale_date"
    }.intersection(h):
        return "travelagency"

    # CORPORATE SALES
    if {
        "corp_id", "client_name", "contract_value",
        "start_date", "end_date", "invoice", "transaction_id"
    }.intersection(h):
        return "corporatesales"

    return None


def detect_entity_from_row(row: Dict[str, Any]) -> Optional[str]:
    return detect_entity_from_headers(list(row.keys()))
