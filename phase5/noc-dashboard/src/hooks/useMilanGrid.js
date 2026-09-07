import { useEffect, useState } from "react";

let cachedGeoJson = null;
let fetchPromise = null; // prevents duplicate in-flight fetches too

export function useMilanGrid() {
  const [geoJson, setGeoJson] = useState(cachedGeoJson);
  const [loading, setLoading] = useState(!cachedGeoJson);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (cachedGeoJson) {
      setGeoJson(cachedGeoJson);
      setLoading(false);
      return;
    }

    if (!fetchPromise) {
      fetchPromise = fetch("/reference/milano-grid.geojson").then((res) => {
        if (!res.ok) throw new Error(`Failed to load grid geometry: ${res.status}`);
        return res.json();
      });
    }

    fetchPromise
      .then((data) => {
        cachedGeoJson = data;
        setGeoJson(data);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  return { geoJson, loading, error };
}