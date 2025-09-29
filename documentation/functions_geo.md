[Back to index](./README.md)

# Geo Functions

---

### Function: ST_DISTANCE (Aliases: DISTANCE)
**Description:** Compute distance between two geo points. Under the hood may use Haversine or ES geo-distance depending on index mapping and push-down capability.
**Inputs:** `point1`, `point2` (WKT or field references)
**Output:** DOUBLE (distance in meters or configured units)
**Example:**
```sql
SELECT ST_DISTANCE(location, 'POINT(2.3522 48.8566)') AS dist FROM places;
-- Result: e.g., 1234.56
```

### Function: ST_WITHIN
**Description:** Test whether point/geometry is within another geometry.
**Inputs:** `geom1`, `geom2`
**Output:** BOOLEAN
**Example:**
```sql
SELECT ST_WITHIN(location, 'POLYGON((...))') FROM places;
```

[Back to index](./README.md)
