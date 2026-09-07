// Builds a Map from grid_id -> GeoJSON feature, using properties.cellId
// as the join key. NEVER use the top-level feature.id — it is
// 0-indexed and shifts every cell by one relative to grid_id.
export function buildGridLookup(geoJson) {
  const lookup = new Map();
  if (!geoJson || !geoJson.features) return lookup;

  for (const feature of geoJson.features) {
    const gridId = feature.properties.cellId;
    lookup.set(gridId, feature);
  }
  return lookup;
}

export function getPolygonPoints(feature) {
  // Polygon coordinates: [ [ [lon,lat], [lon,lat], ... ] ]
  const ring = feature.geometry.coordinates[0];
  return ring;
}