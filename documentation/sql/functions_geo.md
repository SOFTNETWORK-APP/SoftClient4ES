[Back to index](README.md)

## Geo Functions

---

### ST_DISTANCE / DISTANCE

Computes the geodesic distance (great-circle distance) between two points.

**Syntax:**
```sql
ST_DISTANCE(point1, point2)
DISTANCE(point1, point2)  -- Alias
```

**Inputs:**

Each point can be:
- A column of type `geo_point` in Elasticsearch
- A literal defined with `POINT(latitude, longitude)`

**Output:**
- Distance value (numeric) that can be compared with distance literals

**Distance Literals:**

When comparing distances, specify the unit directly after the numeric value:

```sql
value km    -- Kilometers
value m     -- Meters (default if no unit)
value cm    -- Centimeters
value mm    -- Millimeters
value mi    -- Miles
value yd    -- Yards
value ft    -- Feet
value in    -- Inches
value nmi   -- Nautical miles
```

**Performance Note:**

If both arguments are fixed points, the distance is **precomputed at query compilation time**.

---

### Supported Distance Units

| Category     | Unit           | Syntax | Example              |
|--------------|----------------|--------|----------------------|
| **Metric**   | Kilometers     | `km`   | `5000 km`, `10.5 km` |
|              | Meters         | `m`    | `500 m`, `1000 m`    |
|              | Centimeters    | `cm`   | `100 cm`, `50 cm`    |
|              | Millimeters    | `mm`   | `1000 mm`, `500 mm`  |
| **Imperial** | Miles          | `mi`   | `10 mi`, `5.5 mi`    |
|              | Yards          | `yd`   | `100 yd`, `50 yd`    |
|              | Feet           | `ft`   | `500 ft`, `100 ft`   |
|              | Inches         | `in`   | `100 in`, `50 in`    |
| **Nautical** | Nautical Miles | `nmi`  | `50 nmi`, `10 nmi`   |

---

### Examples

**1. Basic distance comparison (kilometers):**
```sql
SELECT 
  name,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 5 km
ORDER BY distance ASC
```

**2. Distance range with BETWEEN (kilometers):**
```sql
SELECT 
  name,
  address,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) BETWEEN 4 km AND 10 km
ORDER BY distance ASC
```

**3. Multiple distance comparisons with different units:**
```sql
SELECT 
  ST_DISTANCE(POINT(-70.0, 40.0), toLocation) AS d1,
  ST_DISTANCE(fromLocation, POINT(-70.0, 40.0)) AS d2,
  ST_DISTANCE(POINT(-70.0, 40.0), POINT(0.0, 0.0)) AS d3
FROM routes
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 km AND 5000 km
  AND ST_DISTANCE(fromLocation, toLocation) < 2000 km
  AND ST_DISTANCE(POINT(-70.0, 40.0), POINT(-70.0, 40.0)) < 1000 km
```

**4. Distance in miles:**
```sql
SELECT 
  name,
  cuisine,
  rating,
  ST_DISTANCE(POINT(40.7128, -74.0060), location) AS distance
FROM restaurants
WHERE ST_DISTANCE(POINT(40.7128, -74.0060), location) <= 2 mi
  AND rating >= 4.0
ORDER BY rating DESC, distance ASC
LIMIT 20
```

**5. Distance in meters:**
```sql
SELECT 
  building_name,
  ST_DISTANCE(POINT(48.8566, 2.3522), entrance_location) AS distance
FROM buildings
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), entrance_location) < 500 m
ORDER BY distance ASC
```

**6. Distance in feet:**
```sql
SELECT 
  landmark_name,
  ST_DISTANCE(POINT(40.7128, -74.0060), location) AS distance
FROM landmarks
WHERE ST_DISTANCE(POINT(40.7128, -74.0060), location) <= 1000 ft
ORDER BY distance ASC
```

**7. Distance in nautical miles:**
```sql
SELECT 
  ship_name,
  DISTANCE(port_location, ship_location) AS distance
FROM vessels
WHERE DISTANCE(port_location, ship_location) < 50 nmi
ORDER BY distance ASC
```

**8. Distance between two fields:**
```sql
SELECT 
  route_id,
  origin_name,
  destination_name,
  ST_DISTANCE(origin_location, destination_location) AS distance_km
FROM routes
WHERE ST_DISTANCE(origin_location, destination_location) > 100 km
  AND ST_DISTANCE(origin_location, destination_location) < 500 km
ORDER BY distance_km DESC
```

**9. Distance between two fixed points (precomputed):**
```sql
-- Paris to New York
SELECT ST_DISTANCE(
  POINT(48.8566, 2.3522),   -- Paris
  POINT(40.7128, -74.0060)  -- New York
) AS distance
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), POINT(40.7128, -74.0060)) > 5000 km
-- Distance is precomputed at query compilation time
```

**10. Multiple distance conditions:**
```sql
SELECT 
  name,
  category,
  ST_DISTANCE(POINT(51.5074, -0.1278), location) AS distance
FROM points_of_interest
WHERE ST_DISTANCE(POINT(51.5074, -0.1278), location) >= 1 km
  AND ST_DISTANCE(POINT(51.5074, -0.1278), location) <= 10 km
  AND category IN ('restaurant', 'cafe', 'bar')
ORDER BY distance ASC
```

**11. Using DISTANCE alias:**
```sql
SELECT 
  name,
  DISTANCE(POINT(48.8566, 2.3522), location) AS dist
FROM hotels
WHERE DISTANCE(POINT(48.8566, 2.3522), location) < 3 km
ORDER BY dist ASC
```

**12. Complex distance query with multiple units:**
```sql
SELECT 
  store_id,
  name,
  ST_DISTANCE(warehouse_location, store_location) AS warehouse_dist,
  ST_DISTANCE(POINT(48.8566, 2.3522), store_location) AS city_center_dist
FROM stores
WHERE ST_DISTANCE(warehouse_location, store_location) < 50 km
  AND ST_DISTANCE(POINT(48.8566, 2.3522), store_location) BETWEEN 5 km AND 20 km
ORDER BY city_center_dist ASC
```

**13. Delivery zone classification:**
```sql
SELECT 
  order_id,
  customer_address,
  ST_DISTANCE(warehouse_location, delivery_location) AS distance,
  CASE
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 5 km THEN 'Zone 1 - Free'
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 15 km THEN 'Zone 2 - Standard'
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 30 km THEN 'Zone 3 - Extended'
    ELSE 'Zone 4 - Premium'
  END AS delivery_zone
FROM orders
WHERE order_date >= CURRENT_DATE
  AND ST_DISTANCE(warehouse_location, delivery_location) < 50 km
ORDER BY distance ASC
```

**14. Geofencing with meters:**
```sql
SELECT 
  device_id,
  user_name,
  timestamp,
  ST_DISTANCE(POINT(48.8566, 2.3522), current_location) AS distance,
  CASE
    WHEN ST_DISTANCE(POINT(48.8566, 2.3522), current_location) <= 50 m THEN 'Inside - Core'
    WHEN ST_DISTANCE(POINT(48.8566, 2.3522), current_location) <= 100 m THEN 'Inside - Buffer'
    WHEN ST_DISTANCE(POINT(48.8566, 2.3522), current_location) <= 500 m THEN 'Nearby'
    ELSE 'Outside'
  END AS geofence_status
FROM device_locations
WHERE timestamp >= NOW() - INTERVAL 15 MINUTE
  AND ST_DISTANCE(POINT(48.8566, 2.3522), current_location) < 1000 m
ORDER BY timestamp DESC
```

**15. Maritime routes (nautical miles):**
```sql
SELECT 
  route_id,
  origin_port,
  destination_port,
  ST_DISTANCE(origin_location, destination_location) AS distance_nmi
FROM shipping_routes
WHERE ST_DISTANCE(origin_location, destination_location) BETWEEN 100 nmi AND 500 nmi
  AND active = true
ORDER BY distance_nmi ASC
```

**16. Aggregation with distance filtering:**
```sql
SELECT 
  category,
  COUNT(*) as store_count,
  AVG(ST_DISTANCE(POINT(48.8566, 2.3522), location)) as avg_distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 20 km
GROUP BY category
HAVING COUNT(*) > 5
ORDER BY avg_distance ASC
```

**17. Nearest locations with limit:**
```sql
SELECT 
  airport_code,
  airport_name,
  city,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM airports
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 200 km
ORDER BY distance ASC
LIMIT 5
```

**18. Distance comparison between multiple points:**
```sql
SELECT 
  location_id,
  name,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS dist_from_paris,
  ST_DISTANCE(POINT(51.5074, -0.1278), location) AS dist_from_london,
  ST_DISTANCE(POINT(52.5200, 13.4050), location) AS dist_from_berlin
FROM european_offices
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 500 km
   OR ST_DISTANCE(POINT(51.5074, -0.1278), location) < 500 km
   OR ST_DISTANCE(POINT(52.5200, 13.4050), location) < 500 km
ORDER BY dist_from_paris ASC
```

---

### POINT

Creates a geo-point from latitude and longitude.

**Syntax:**
```sql
POINT(latitude, longitude)
```

**Inputs:**
- `latitude` - DOUBLE (-90 to 90)
  - Positive values: North
  - Negative values: South
- `longitude` - DOUBLE (-180 to 180)
  - Positive values: East
  - Negative values: West

**Output:**
- `geo_point` type

**Examples:**

**1. Create a point:**
```sql
SELECT POINT(48.8566, 2.3522) as paris_location
-- Paris: 48.8566°N, 2.3522°E
```

**2. Major world cities:**
```sql
-- Paris, France
SELECT POINT(48.8566, 2.3522) as paris

-- London, UK
SELECT POINT(51.5074, -0.1278) as london

-- New York, USA
SELECT POINT(40.7128, -74.0060) as new_york

-- Tokyo, Japan
SELECT POINT(35.6762, 139.6503) as tokyo

-- Sydney, Australia
SELECT POINT(-33.8688, 151.2093) as sydney
```

**3. Use in distance calculation:**
```sql
-- Distance between Paris and London
SELECT 
  ST_DISTANCE(POINT(48.8566, 2.3522), POINT(51.5074, -0.1278)) AS distance
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), POINT(51.5074, -0.1278)) < 400 km
-- Result: precomputed distance, approximately 343 km
```

**4. Use in WHERE clause:**
```sql
SELECT *
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 5 km
```

---

### Practical Geo-Spatial Examples

**1. Store Locator (within 5km):**
```sql
SELECT 
  name,
  address,
  phone,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) <= 5 km
  AND open_now = true
ORDER BY distance ASC
LIMIT 10
```

**2. Restaurant Finder (within 1 mile):**
```sql
SELECT 
  name,
  cuisine,
  rating,
  ST_DISTANCE(POINT(40.7128, -74.0060), location) AS distance
FROM restaurants
WHERE ST_DISTANCE(POINT(40.7128, -74.0060), location) <= 1 mi
  AND rating >= 4.0
ORDER BY rating DESC, distance ASC
LIMIT 20
```

**3. Delivery Zone Analysis:**
```sql
SELECT 
  order_id,
  customer_name,
  ST_DISTANCE(warehouse_location, delivery_location) AS distance,
  CASE
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 5 km THEN 'Local - Free Delivery'
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 20 km THEN 'Regional - 5€'
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 50 km THEN 'Extended - 15€'
    ELSE 'Long Distance - 30€'
  END AS delivery_zone
FROM orders
WHERE order_date >= CURRENT_DATE
  AND ST_DISTANCE(warehouse_location, delivery_location) < 100 km
ORDER BY distance ASC
```

**4. Nearest Airport Search:**
```sql
SELECT 
  airport_code,
  airport_name,
  city,
  country,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM airports
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 100 km
ORDER BY distance ASC
LIMIT 5
```

**5. Maritime Route Planning:**
```sql
SELECT 
  route_id,
  origin_port,
  destination_port,
  ST_DISTANCE(origin_location, destination_location) AS distance_nmi
FROM shipping_routes
WHERE ST_DISTANCE(origin_location, destination_location) BETWEEN 50 nmi AND 200 nmi
  AND active = true
ORDER BY distance_nmi ASC
```

**6. Coverage Analysis by Store:**
```sql
SELECT 
  s.store_id,
  s.name as store_name,
  s.city,
  COUNT(DISTINCT c.customer_id) as customers_within_5km,
  COUNT(DISTINCT CASE 
    WHEN ST_DISTANCE(s.location, c.location) <= 2 km 
    THEN c.customer_id 
  END) as customers_within_2km
FROM stores s
JOIN customers c ON ST_DISTANCE(s.location, c.location) <= 5 km
WHERE s.active = true
GROUP BY s.store_id, s.name, s.city
ORDER BY customers_within_5km DESC
```

**7. Real-time Geofencing:**
```sql
SELECT 
  device_id,
  user_name,
  timestamp,
  ST_DISTANCE(POINT(48.8566, 2.3522), current_location) AS distance,
  CASE
    WHEN ST_DISTANCE(POINT(48.8566, 2.3522), current_location) <= 50 m THEN 'Inside'
    WHEN ST_DISTANCE(POINT(48.8566, 2.3522), current_location) <= 100 m THEN 'Near'
    ELSE 'Outside'
  END AS status
FROM device_locations
WHERE timestamp >= NOW() - INTERVAL 15 MINUTE
  AND ST_DISTANCE(POINT(48.8566, 2.3522), current_location) < 500 m
ORDER BY timestamp DESC
```

**8. Multi-Distance Route Filtering:**
```sql
SELECT 
  route_id,
  origin_city,
  destination_city,
  ST_DISTANCE(origin_location, destination_location) AS route_distance,
  ST_DISTANCE(POINT(48.8566, 2.3522), origin_location) AS origin_from_paris,
  ST_DISTANCE(POINT(48.8566, 2.3522), destination_location) AS dest_from_paris
FROM routes
WHERE ST_DISTANCE(origin_location, destination_location) BETWEEN 100 km AND 500 km
  AND (ST_DISTANCE(POINT(48.8566, 2.3522), origin_location) < 50 km
       OR ST_DISTANCE(POINT(48.8566, 2.3522), destination_location) < 50 km)
ORDER BY route_distance ASC
```

**9. Distance-Based Pricing:**
```sql
SELECT 
  order_id,
  customer_name,
  ST_DISTANCE(warehouse_location, delivery_location) AS distance,
  CASE
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 10 km THEN 5.00
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 25 km THEN 10.00
    WHEN ST_DISTANCE(warehouse_location, delivery_location) <= 50 km THEN 20.00
    ELSE 35.00
  END AS shipping_cost
FROM orders
WHERE order_date >= CURRENT_DATE
  AND ST_DISTANCE(warehouse_location, delivery_location) < 100 km
ORDER BY distance ASC
```

**10. Proximity Search with Multiple Criteria:**
```sql
SELECT 
  hotel_id,
  name,
  rating,
  price_per_night,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance_from_center,
  ST_DISTANCE(airport_location, location) AS distance_from_airport
FROM hotels
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 5 km
  AND ST_DISTANCE(airport_location, location) < 30 km
  AND rating >= 4.0
  AND price_per_night <= 200
ORDER BY rating DESC, distance_from_center ASC
LIMIT 10
```

---

### Common Distance Thresholds by Unit

```sql
-- Kilometers
WHERE ST_DISTANCE(point1, point2) < 1 km       -- 1 kilometer
WHERE ST_DISTANCE(point1, point2) <= 5 km      -- 5 kilometers
WHERE ST_DISTANCE(point1, point2) BETWEEN 10 km AND 50 km

-- Miles
WHERE ST_DISTANCE(point1, point2) < 1 mi       -- 1 mile
WHERE ST_DISTANCE(point1, point2) <= 5 mi      -- 5 miles
WHERE ST_DISTANCE(point1, point2) BETWEEN 10 mi AND 50 mi

-- Meters
WHERE ST_DISTANCE(point1, point2) < 100 m      -- 100 meters
WHERE ST_DISTANCE(point1, point2) <= 500 m     -- 500 meters
WHERE ST_DISTANCE(point1, point2) BETWEEN 1000 m AND 5000 m

-- Feet
WHERE ST_DISTANCE(point1, point2) < 100 ft     -- 100 feet
WHERE ST_DISTANCE(point1, point2) <= 500 ft    -- 500 feet
WHERE ST_DISTANCE(point1, point2) BETWEEN 1000 ft AND 5000 ft

-- Nautical Miles
WHERE ST_DISTANCE(point1, point2) < 10 nmi     -- 10 nautical miles
WHERE ST_DISTANCE(point1, point2) <= 50 nmi    -- 50 nautical miles
WHERE ST_DISTANCE(point1, point2) BETWEEN 100 nmi AND 500 nmi

-- Yards
WHERE ST_DISTANCE(point1, point2) < 100 yd     -- 100 yards
WHERE ST_DISTANCE(point1, point2) <= 500 yd    -- 500 yards

-- Centimeters
WHERE ST_DISTANCE(point1, point2) < 100 cm     -- 100 centimeters
WHERE ST_DISTANCE(point1, point2) <= 500 cm    -- 500 centimeters

-- Millimeters
WHERE ST_DISTANCE(point1, point2) < 1000 mm    -- 1000 millimeters
WHERE ST_DISTANCE(point1, point2) <= 5000 mm   -- 5000 millimeters

-- Inches
WHERE ST_DISTANCE(point1, point2) < 100 in     -- 100 inches
WHERE ST_DISTANCE(point1, point2) <= 500 in    -- 500 inches
```

---

### Unit Selection Guidelines

**Choose the appropriate unit based on your use case:**

| Use Case           | Recommended Unit  | Example Query                     |
|--------------------|-------------------|-----------------------------------|
| City/Urban search  | `km` or `m`       | `WHERE ST_DISTANCE(...) < 5 km`   |
| Country/Regional   | `km` or `mi`      | `WHERE ST_DISTANCE(...) < 100 km` |
| Maritime/Aviation  | `nmi`             | `WHERE ST_DISTANCE(...) < 50 nmi` |
| Building/Indoor    | `m` or `ft`       | `WHERE ST_DISTANCE(...) < 100 m`  |
| Precision tracking | `m` or `cm`       | `WHERE ST_DISTANCE(...) < 50 m`   |
| US-based apps      | `mi` or `ft`      | `WHERE ST_DISTANCE(...) < 10 mi`  |
| International apps | `km` or `m`       | `WHERE ST_DISTANCE(...) < 10 km`  |

---
```

---

## **README.md mis à jour**

```markdown
### Geo-Spatial Queries

The API supports comprehensive geo-spatial queries with distance calculations:

```sql
-- Distance in kilometers
SELECT 
  name,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) < 5 km
ORDER BY distance ASC

-- Distance in miles
SELECT 
  name,
  ST_DISTANCE(POINT(40.7128, -74.0060), location) AS distance
FROM restaurants
WHERE ST_DISTANCE(POINT(40.7128, -74.0060), location) <= 2 mi
ORDER BY distance ASC

-- Distance range with BETWEEN
SELECT 
  name,
  ST_DISTANCE(POINT(48.8566, 2.3522), location) AS distance
FROM stores
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), location) BETWEEN 4 km AND 10 km

-- Multiple distance comparisons
SELECT 
  ST_DISTANCE(POINT(-70.0, 40.0), toLocation) AS d1,
  ST_DISTANCE(fromLocation, POINT(-70.0, 40.0)) AS d2
FROM routes
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 km AND 5000 km
  AND ST_DISTANCE(fromLocation, toLocation) < 2000 km

-- Distance in nautical miles
SELECT 
  ship_name,
  DISTANCE(port_location, ship_location) AS distance
FROM vessels
WHERE DISTANCE(port_location, ship_location) < 50 nmi

-- Distance in meters for precision
SELECT 
  device_id,
  ST_DISTANCE(POINT(48.8566, 2.3522), current_location) AS distance
FROM devices
WHERE ST_DISTANCE(POINT(48.8566, 2.3522), current_location) < 100 m
```

**Supported Distance Units:**
- **Metric**: `km`, `m`, `cm`, `mm`
- **Imperial**: `mi`, `yd`, `ft`, `in`
- **Nautical**: `nmi`

**Syntax**: Specify the unit directly after the numeric value in comparisons.

[Back to index](README.md)
