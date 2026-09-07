import { createContext, useContext, useEffect, useState } from "react";

const MilanGridContext = createContext(null);

export function MilanGridProvider({ children }) {
  const [geoJson, setGeoJson] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch("/reference/milano-grid.geojson")
      .then((res) => {
        if (!res.ok) throw new Error(`Failed to load grid geometry: ${res.status}`);
        return res.json();
      })
      .then((data) => setGeoJson(data))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []); // runs exactly once for the entire app lifetime — provider mounts only once

  return (
    <MilanGridContext.Provider value={{ geoJson, loading, error }}>
      {children}
    </MilanGridContext.Provider>
  );
}

export function useMilanGrid() {
  const context = useContext(MilanGridContext);
  if (!context) {
    throw new Error("useMilanGrid must be used within a MilanGridProvider");
  }
  return context;
}