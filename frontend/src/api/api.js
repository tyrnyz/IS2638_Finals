// frontend/src/api/api.js
const BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

/**
 * Upload using XMLHttpRequest to allow upload progress callbacks.
 * @param {File} file - file object
 * @param {string} dataset - 'airline' | 'passenger' | 'flight'
 * @param {(pct:number) => void} onProgress - optional progress callback
 * @returns {Promise<any>}
 */
export function uploadFile(file, dataset = "airline", onProgress = null) {
  return new Promise((resolve, reject) => {
    const url = `${BASE}/api/upload`;
    const form = new FormData();
    form.append("file", file);
    form.append("dataset", dataset);

    const xhr = new XMLHttpRequest();
    xhr.open("POST", url, true);

    xhr.onload = function () {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const json = JSON.parse(xhr.responseText);
          resolve(json);
        } catch (err) {
          resolve(xhr.responseText);
        }
      } else {
        reject(new Error(`Upload failed: ${xhr.status} ${xhr.statusText} - ${xhr.responseText}`));
      }
    };

    xhr.onerror = function () {
      reject(new Error("Network error during file upload"));
    };

    if (xhr.upload && onProgress) {
      xhr.upload.onprogress = function (event) {
        if (event.lengthComputable) {
          const pct = Math.round((event.loaded / event.total) * 100);
          try { onProgress(pct); } catch (e) {}
        }
      };
    }

    xhr.send(form);
  });
}

/**
 * Simple fetch version (no progress)
 */
export async function uploadFileSimple(file, dataset = "airline") {
  const url = `${BASE}/api/upload`;
  const form = new FormData();
  form.append("file", file);
  form.append("dataset", dataset);

  const res = await fetch(url, {
    method: "POST",
    body: form,
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upload failed: ${res.status} ${text}`);
  }
  return res.json();
}