const API_BASE_URL = process.env.REACT_APP_API_BASE_URL;

if (!API_BASE_URL) {
  throw new Error(
    "REACT_APP_API_BASE_URL is not set. Create a .env file with REACT_APP_API_BASE_URL=http://127.0.0.1:8000"
  );
}

export async function apiGet(path) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({}));
    throw new Error(errorBody.detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

export async function apiPost(path, body) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({}));
    throw new Error(errorBody.detail || `Request failed: ${response.status}`);
  }
  return response.json();
}