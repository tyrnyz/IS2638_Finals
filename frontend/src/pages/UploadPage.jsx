// frontend/src/pages/UploadPage.jsx
import React, { useRef, useState } from "react";
import { uploadFile } from "../api/api";

export default function UploadPage() {
  const fileRef = useRef();
  const [progress, setProgress] = useState(0);
  const [checkingServer, setCheckingServer] = useState(false);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);

  const fakeProgress = (target = 100, speed = 1200) => {
    setProgress(0);
    const start = Date.now();
    const id = setInterval(() => {
      const elapsed = Date.now() - start;
      const pct = Math.min(target, Math.floor((elapsed / speed) * target));
      setProgress(pct);
      if (pct >= target) clearInterval(id);
    }, 80);
    return id;
  };

  const uploadFileClick = async () => {
    const file = fileRef.current.files?.[0];
    if (!file) return alert("Please select the file first (CSV or DOCX).");
    setBusy(true);
    setResult(null);

    // small fake progress until upload events arrive
    const fakeId = fakeProgress(20, 800);

    try {
      const dataset = document.getElementById("dataset")?.value || "airline";
      const res = await uploadFile(file, dataset, (pct) => {
        clearInterval(fakeId);
        setProgress(pct);
      });

      setProgress(100);
      setResult({ ok: true, message: "Upload succeeded", detail: res });
    } catch (err) {
      clearInterval(fakeId);
      setProgress(0);
      setResult({ ok: false, message: err.message || String(err) });
    } finally {
      setBusy(false);
    }
  };

  const processClick = () => {
    if (!fileRef.current.files?.length) return alert("No file to process.");
    setResult(null);
    if (checkingServer) {
      setProgress(15);
      setTimeout(() => setProgress(60), 500);
      setTimeout(() => setProgress(100), 1100);
    } else {
      setProgress(100);
    }
  };

  const verifyEligibility = () => {
    const name = document.getElementById("name")?.value.trim();
    const flight = document.getElementById("flight")?.value.trim();
    const baggage = Number(document.getElementById("baggage")?.value || 0);
    if (!name || !flight) {
      setResult({ ok: false, message: "Missing name or flight #" });
      return;
    }
    setResult(baggage >= 1 ? { ok: true, message: "Customer is eligible!" } : { ok: false, message: "Customer is NOT eligible — no baggage recorded." });
  };

  return (
    <div className="card-large">
      <div className="accent">Upload & Verify</div>

      <div className="columns">
        <div className="left-panel">
          <div style={{ fontWeight: 700, color: "var(--accent)", marginBottom: 8 }}>Upload file (CSV or DOCX)</div>

          <div className="upload-area">
            <div className="muted">Upload file</div>

            <div style={{ marginTop: 8, display: "flex", gap: 8 }}>
              <input
                ref={fileRef}
                type="file"
                accept=".csv, .docx, application/vnd.openxmlformats-officedocument.wordprocessingml.document, text/csv"
                aria-label="Upload file (CSV or DOCX)"
              />

              <select id="dataset" defaultValue="airline" style={{ padding: 6 }}>
                <option value="airline">Airline</option>
                <option value="passenger">Passenger</option>
                <option value="flight">Flight</option>
              </select>
            </div>

            <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 10 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <input type="checkbox" checked={checkingServer} onChange={e => setCheckingServer(e.target.checked)} />
                <span style={{ fontSize: 14 }}>Check in server</span>
              </label>

              <button className="btn-primary" onClick={uploadFileClick} disabled={busy}>Upload</button>
              <button className="btn-ghost" onClick={processClick} disabled={busy}>Process</button>
            </div>

            <div style={{ marginTop: 12 }}>
              <div className="muted">Progress</div>
              <div className="progress-bar" style={{ background: "#eee", height: 10, borderRadius: 6 }}>
                <div className="progress-fill" style={{ width: `${progress}%`, height: "100%", background: "linear-gradient(90deg,#4caf50,#81c784)", borderRadius: 6 }} />
              </div>
            </div>
          </div>

          <div style={{ marginTop: 18 }}>
            <div style={{ fontWeight: 700, color: "var(--accent)", marginBottom: 6 }}>Insurance Eligibility (example)</div>

            <div className="card">
              <div style={{ marginBottom: 8 }}>
                <label className="muted">Name</label>
                <input id="name" placeholder="Customer name" style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #ddd" }} />
              </div>

              <div style={{ marginBottom: 8 }}>
                <label className="muted">Flight #</label>
                <input id="flight" placeholder="e.g. PR1234" style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #ddd" }} />
              </div>

              <div style={{ marginBottom: 8 }}>
                <label className="muted">Baggage</label>
                <input id="baggage" type="number" min="0" placeholder="# of bags" style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #ddd" }} />
              </div>

              <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 8 }}>
                <button className="btn-primary" onClick={verifyEligibility}>Verify</button>
                <div style={{ minWidth: 200 }}>
                  {result && (
                    <div className={`result ${result.ok ? "eligible" : "not"}`}>{result.message}</div>
                  )}
                </div>
                <div className="exclaim">!</div>
              </div>
            </div>
          </div>

          <div style={{ marginTop: 12, color: "#666", fontSize: 14 }}>
            <strong style={{ color: "var(--accent)" }}>Customer is eligible!</strong>
          </div>
        </div>

        <div className="right-panel">
          <div style={{ fontWeight: 700, color: "var(--accent)" }}>Rules / Notes</div>

          <div className="note" style={{ marginTop: 8 }}>
            <div style={{ fontWeight: 700, marginBottom: 6 }}>Other checks</div>
            <ul className="muted">
              <li>Is flight covered by policy?</li>
              <li>Has claim been previously submitted?</li>
              <li>Required documents uploaded?</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}