[Back to index](./README.md)

# Geo Functions

---

### Function: ST_DISTANCE (Alias: DISTANCE)
**Description:** 

Computes the geodesic distance (great-circle distance) in meters between two points.

**Inputs:**

Each point can be:
- A column of type `geo_point` in Elasticsearch
- A literal defined with `POINT(lat, lon)`

If both arguments are fixed points, the distance is **precomputed at query compilation time**.

**Output:**
- `DOUBLE` (distance in meters)

**Examples:**

- Distance between a fixed point and a field
```sql
  SELECT ST_DISTANCE(POINT(-70.0, 40.0), toLocation) AS d
  FROM locations;
```
- Distance between two fields
```sql
SELECT ST_DISTANCE(fromLocation, toLocation) AS d
FROM locations;
```
- Distance between two fixed points (precomputed)
```sql
SELECT ST_DISTANCE(
    POINT(-70.0, 40.0), 
    POINT(0.0, 0.0)
) AS d;
  -- Precomputed result: 8318612.0 (meters)
```

[Back to index](./README.md)
