// frontend/src/api/api.js
// Minimal, robust API helpers for upload + process
// Replace your existing api.js with this file.

export function _safeParseResponseText(respText) {
  // Try JSON first, otherwise return raw text
  if (!respText) return { ok: false, text: "" };
  try {
    const parsed = JSON.parse(respText);
    return { ok: true, json: parsed, text: respText };
  } catch (err) {
    return { ok: false, json: null, text: respText };
  }
}

/**
 * Upload a file to the backend /api/upload endpoint.
 * - file: File object
 * - dataset: string dataset key (airline/airport/flight/etc)
 * - onProgress: optional callback(percentNumber 0-100)
 *
 * Returns a Promise that resolves to an object:
 * { success: boolean, status: number, data: <parsed JSON or raw text>, error?: string, upload_id?: number }
 */
export function uploadFile(file, dataset, onProgress) {
  return new Promise((resolve, reject) => {
    if (!file) {
      return resolve({ success: false, error: "no file provided" });
    }

    const url = "/api/upload";
    const form = new FormData();
    form.append("file", file, file.name);
    form.append("dataset", dataset || "airline");

    const xhr = new XMLHttpRequest();
    xhr.open("POST", url, true);

    xhr.upload.onprogress = function (ev) {
      if (ev.lengthComputable && typeof onProgress === "function") {
        const pct = Math.round((ev.loaded / ev.total) * 100);
        try { onProgress(pct); } catch (_) {}
      }
    };

    xhr.onload = function () {
      const status = xhr.status;
      const text = xhr.responseText;
      const parsed = _safeParseResponseText(text);

      if (status >= 200 && status < 300) {
        if (parsed.ok && parsed.json) {
          const data = parsed.json;
          return resolve({
            success: true,
            status,
            data,
            upload_id: data?.upload_id ?? data?.uploadId ?? null,
          });
        } else {
          // 200 but non-json body (e.g. plain text) — treat as success but surface text
          return resolve({ success: true, status, data: parsed.text, upload_id: null });
        }
      }

      // non-2xx
      const errMsg = parsed.ok && parsed.json ? JSON.stringify(parsed.json) : parsed.text || `HTTP ${status}`;
      return resolve({ success: false, status, error: errMsg, data: parsed.json ?? parsed.text });
    };

    xhr.onerror = function () {
      return resolve({ success: false, error: "Network error during upload" });
    };

    xhr.onabort = function () {
      return resolve({ success: false, error: "Upload aborted" });
    };

    xhr.send(form);
  });
}

/**
 * Trigger processing for an upload.
 * - uploadId: integer upload id returned by /api/upload
 * - detected: optional dataset key (fallback to 'dataset' column in staging)
 *
 * Returns Promise resolving to { success, status, data, error }
 */
export async function processUpload(uploadId, detected) {
  const url = "/api/process";
  const form = new URLSearchParams();
  if (uploadId != null) form.append("upload_id", String(uploadId));
  if (detected) form.append("dataset", detected);

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: form.toString(),
    });

    const text = await resp.text();
    const parsed = _safeParseResponseText(text);

    if (resp.ok) {
      return {
        success: true,
        status: resp.status,
        data: parsed.ok ? parsed.json : parsed.text,
      };
    } else {
      return {
        success: false,
        status: resp.status,
        error: parsed.ok ? JSON.stringify(parsed.json) : parsed.text || `HTTP ${resp.status}`,
        data: parsed.ok ? parsed.json : parsed.text,
      };
    }
  } catch (err) {
    return { success: false, error: String(err) };
  }
}
