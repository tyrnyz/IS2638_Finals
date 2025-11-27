// frontend/src/pages/UploadPage.jsx
import React, { useRef, useState } from "react";
import { uploadAirlinesFile } from "../api/api";

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

  const uploadFile = async () => {
    const file = fileRef.current.files?.[0];
    if (!file) return alert("Please select the airlines file first.");
    setBusy(true);
    setResult(null);
    const id = fakeProgress(90, 1400);

    try {
      const res = await uploadAirlinesFile(file);
      clearInterval(id);
      if (checkingServer) {
        setProgress(15);
        setTimeout(() => setProgress(60), 500);
        setTimeout(() => setProgress(100), 1000);
      } else {
        setProgress(100);
      }
      setResult({ ok: true, message: "Upload succeeded", detail: res.result || res });
    } catch (err) {
      clearInterval(id);
      setProgress(0);
      setResult({ ok: false, message: err.message });
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
      <div className="accent">Airlines — Upload & Verify</div>

      <div className="columns">
        <div className="left-panel">
          <div style={{ fontWeight: 700, color: "var(--accent)", marginBottom: 8 }}>Upload airlines file</div>

          <div className="upload-area">
            <div className="muted">Upload file</div>
            <input ref={fileRef} type="file" aria-label="Upload airlines file" style={{ marginTop: 8 }} />

            <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 10 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <input type="checkbox" checked={checkingServer} onChange={e => setCheckingServer(e.target.checked)} />
                <span style={{ fontSize: 14 }}>Check in server</span>
              </label>

              <button className="btn-primary" onClick={uploadFile} disabled={busy}>Upload</button>

              <button className="btn-ghost" onClick={processClick} disabled={busy}>Process</button>
            </div>

            <div style={{ marginTop: 12 }}>
              <div className="muted">Progress</div>
              <div className="progress-bar">
                <div className="progress-fill" style={{ width: `${progress}%` }} />
              </div>
            </div>
          </div>

          <div style={{ marginTop: 18 }}>
            <div style={{ fontWeight: 700, color: "var(--accent)", marginBottom: 6 }}>Insurance Eligibility</div>

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

              <div style={{ marginBottom: 8 }}>
                <label className="muted">Date</label>
                <input id="date" type="date" style={{ width: "100%", padding: 8, borderRadius: 6, border: "1px solid #ddd" }} />
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

          <div className="note">
            <div style={{ fontWeight: 700, marginBottom: 6 }}>Flight delayed by &gt; 4 hrs</div>
            <div className="muted">If flight is delayed more than 4 hours — the customer could be eligible for compensation (example rule).</div>
          </div>

          <div className="note" style={{ marginTop: 8 }}>
            <div style={{ fontWeight: 700, marginBottom: 6 }}>Other checks</div>
            <ul className="muted">
              <li>Is flight covered by policy?</li>
              <li>Has claim been previously submitted?</li>
              <li>Required documents uploaded?</li>
            </ul>
          </div>

          <div style={{ marginTop: 18, paddingTop: 12, borderTop: "1px dashed rgba(0,0,0,0.05)", color: "#666" }}>
            <div style={{ fontWeight: 700, color: "var(--accent)", marginBottom: 6 }}>Designer scribble</div>
            <div>Sketchy notes: "Disaster is cold", "Success is damaged" — (placeholder).</div>
          </div>
        </div>
      </div>
    </div>
  );
}