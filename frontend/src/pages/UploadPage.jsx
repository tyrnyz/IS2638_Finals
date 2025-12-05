import React, { useRef, useState } from "react";

export default function UploadPage() {
  const fileRef = useRef();

  const [progress, setProgress] = useState(0);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);

  const [detected, setDetected] = useState(null);
  const [lastUploadId, setLastUploadId] = useState(null);

  // debug: show tokenized header
  const [tokensPreview, setTokensPreview] = useState(null);

  // -------------------------
  // Detector helpers
  // -------------------------
  const tokenizeHeader = (line = "") =>
    String(line || "")
      .split(/[,;\t|]/)
      .map((s) => s.trim().toLowerCase().replace(/[\s\-]+/g, "_"))
      .filter(Boolean)
      .map((t) => {
        // canonicalize common legacy tokens
        if (["iata", "iata_code", "airport_code"].includes(t)) return "airportkey";
        if (["icao", "icao_code"].includes(t)) return "icao";
        return t;
      });

  const KEYWORDS = {
    airline: ["airline", "airlinekey", "airlinename"],
    passenger: ["passenger", "passengerkey", "fullname"],
    flight: ["flightkey", "originairportkey", "destinationairportkey"],
    airport: ["airportkey", "airport", "airportname", "city", "country", "icao"],
    travelagency: ["agency", "booking", "saleamount"],
    corporatesales: ["invoice", "transactionid", "corporate"],
  };

  const pickBestMatch = (tokens = [], filename = "") => {
    const scores = {};
    for (const [ds, keys] of Object.entries(KEYWORDS)) {
      let score = 0;
      for (const t of tokens) for (const k of keys) if (t.includes(k)) score++;
      for (const k of keys) if (filename.toLowerCase().includes(k.replace(/_/g, ""))) score += 0.7;
      scores[ds] = score;
    }
    const best = Object.entries(scores).sort((a, b) => b[1] - a[1])[0];
    return best && best[1] > 0 ? best[0] : "airline";
  };

  const detectDatasetFromCSV = (file) =>
    new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = String(e.target.result || "");
        const header = text.split(/\r?\n/).find((l) => l && l.trim()) || "";
        const tokens = tokenizeHeader(header);
        setTokensPreview(tokens);
        resolve(tokens);
      };
      reader.readAsText(file.slice(0, 65536));
    });

  const detectDatasetFromFile = async (file) => {
    const name = String(file.name || "").toLowerCase();
    if (name.endsWith(".csv")) {
      const tokens = await detectDatasetFromCSV(file);
      return pickBestMatch(tokens, name);
    }
    if (name.includes("airport")) return "airport";
    if (name.includes("airline")) return "airline";
    if (name.includes("passeng")) return "passenger";
    if (name.includes("flight")) return "flight";
    if (name.includes("booking") || name.includes("travel")) return "travelagency";
    if (name.includes("corp") || name.includes("invoice")) return "corporatesales";

    // fallback: try to read some text
    try {
      const txt = await file.text();
      const first = txt.split(/\r?\n/).find((l) => l && l.trim()) || "";
      const tokens = tokenizeHeader(first);
      setTokensPreview(tokens);
      return pickBestMatch(tokens, name);
    } catch {
      setTokensPreview(null);
      return "airline";
    }
  };

  // -------------------------
  // Minimal API helpers (direct fetch) — robust JSON/text handling
  // -------------------------
  // Note: fetch does not provide upload progress easily without XHR.
  // We call onProgress(100) when complete.

  // Use explicit backend host to avoid Vite dev server returning 404
  const BACKEND = "http://localhost:8000";


  const uploadFile = async (file, dataset, onProgress = () => {}) => {
    const form = new FormData();
    form.append("file", file);
    form.append("dataset", dataset);

    const res = await fetch(`${BACKEND}/upload`, {
      method: "POST",
      body: form,
    });

    // Always read text first to avoid .json() throwing on empty/non-JSON responses
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (e) {
      json = null; // non-JSON body (HTML, plain text, empty)
    }

    if (!res.ok) {
      // Prefer structured error messages if present
      const errMsg = (json && (json.detail || json.message || json.error)) || text || `HTTP ${res.status}`;
      throw new Error(errMsg);
    }

    onProgress(100);
    // return both parsed payload (if present) and raw text
    return { payload: json, rawText: text };
  };

  const processUpload = async (uploadId, dataset) => {
    const form = new FormData();
    form.append("upload_id", String(uploadId));
    if (dataset) form.append("dataset", dataset);

    const res = await fetch(`${BACKEND}/api/process`, {
      method: "POST",
      body: form,
    });

    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (e) {
      json = null;
    }

    if (!res.ok) {
      const errMsg = (json && (json.detail || json.message || json.error)) || text || `HTTP ${res.status}`;
      throw new Error(errMsg);
    }

    return { payload: json, rawText: text };
  };

  // -------------------------
  // Upload & Process handlers
  // -------------------------
  const handleUpload = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return alert("Select a file first.");

    setBusy(true);
    setProgress(0);
    setResult(null);

    try {
      const dataset = await detectDatasetFromFile(file);
      setDetected(dataset);

      const uploadRes = await uploadFile(file, dataset, (pct) => setProgress(pct));
      // uploadRes.payload is parsed JSON if server returned JSON
      // store upload id if available, otherwise null
      const uploadId = uploadRes.payload?.upload_id ?? null;
      setLastUploadId(uploadId);

      setResult({
        ok: true,
        message: "Upload successful!",
        detail: uploadRes.payload ?? { raw: uploadRes.rawText ?? "(no body)" },
      });
    } catch (err) {
      const detail = err?.message ? err.message : String(err);
      setResult({ ok: false, message: "Upload failed", detail });
    } finally {
      setBusy(false);
    }
  };

  const handleProcess = async () => {
    if (!lastUploadId) return alert("No uploaded file found. Upload first.");

    setBusy(true);
    setProgress(0);
    setResult(null);

    try {
      setProgress(25);
      const processRes = await processUpload(lastUploadId, detected);
      setProgress(100);
      setResult({
        ok: true,
        message: "Processing succeeded!",
        detail: processRes.payload ?? { raw: processRes.rawText ?? "(no body)" },
      });
    } catch (err) {
      const detail = err?.message ? err.message : String(err);
      setResult({ ok: false, message: "Processing failed", detail });
    } finally {
      setBusy(false);
    }
  };

  // -------------------------
  // UI
  // -------------------------
  return (
    <div className="card-large">
      <div className="accent">Upload & ETL</div>

      <div className="columns" style={{ gap: 20 }}>
        <div className="left-panel">
          <div style={{ marginBottom: 8, fontWeight: 700 }}>Upload a CSV or DOCX file</div>

          <input ref={fileRef} type="file" accept=".csv,.docx" />

          <div style={{ marginTop: 8 }}>
            <div className="muted">Detected dataset</div>
            <div style={{ fontWeight: 700, color: "var(--accent)" }}>{detected || "— none —"}</div>

            {tokensPreview && (
              <div style={{ marginTop: 6, fontSize: 12, color: "#888" }}>
                Tokens: <code>{tokensPreview.join(", ")}</code>
              </div>
            )}
          </div>

          <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
            <button className="btn-primary" onClick={handleUpload} disabled={busy}>
              Upload
            </button>

            <button className="btn-secondary" onClick={handleProcess} disabled={busy}>
              Process
            </button>
          </div>

          <div style={{ marginTop: 12 }}>
            <div className="muted">Progress</div>
            <div style={{ background: "#eee", height: 10, borderRadius: 6 }}>
              <div
                style={{
                  width: `${progress}%`,
                  height: "100%",
                  background: "#4caf50",
                  borderRadius: 6,
                  transition: "width 0.2s ease",
                }}
              />
            </div>
          </div>

          {result && (
            <div
              style={{
                marginTop: 14,
                fontWeight: 700,
                color: result.ok ? "#2e7d32" : "#c62828",
              }}
            >
              {result.message}
              {result.detail && (
                <pre
                  style={{
                    background: "#fafafa",
                    padding: 10,
                    borderRadius: 6,
                    marginTop: 6,
                    fontSize: 12,
                    whiteSpace: "pre-wrap",
                  }}
                >
                  {typeof result.detail === "string" ? result.detail : JSON.stringify(result.detail, null, 2)}
                </pre>
              )}
            </div>
          )}
        </div>

        <div className="right-panel">
          <div style={{ fontWeight: 700, color: "var(--accent)" }}>Notes</div>
          <div className="muted" style={{ marginTop: 8 }}>
            Upload → creates staging_raw row
            <br />
            Process → runs ETL, loads cleaned_*, dim_*
          </div>
        </div>
      </div>
    </div>
  );
}
