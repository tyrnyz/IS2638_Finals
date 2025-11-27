const API_URL = import.meta.env.VITE_API_URL || "";

export async function uploadAirlinesFile(file) {
  const fd = new FormData();
  fd.append("file", file);

  const res = await fetch(`${API_URL}/api/airlines/upload`, {
    method: "POST",
    body: fd
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Upload failed: ${res.status} ${text}`);
  }
  return await res.json();
}