"""Shared analytical SQL (Q1–Q6) used by gold products and benchmarks."""

from __future__ import annotations

# View name expected by all queries
INTEGRATED_VIEW = "integrated_taxi_trips"

QUERY_1_MONTHLY_ZONE_DEMAND = """
WITH monthly_zone_trips AS (
    SELECT
        TRUNC(pickup_date, 'MM') AS trip_month,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        pickup_date
    FROM integrated_taxi_trips
    WHERE pickup_zone != 'UNKNOWN'
      AND pickup_date IS NOT NULL
)
SELECT
    trip_month,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT pickup_date) AS active_days,
    ROUND(
        CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT pickup_date),
        2
    ) AS avg_daily_trips
FROM monthly_zone_trips
GROUP BY trip_month, pickup_location_id, pickup_borough, pickup_zone
ORDER BY trip_month ASC, total_trips DESC
"""

QUERY_2_WEATHER_DISTANCE = """
WITH binned_weather AS (
    SELECT
        trip_distance,
        CASE
            WHEN temperature_c IS NULL THEN 'Unknown'
            WHEN temperature_c < 0 THEN 'Freezing (<0°C)'
            WHEN temperature_c BETWEEN 0 AND 10 THEN 'Cold (0°C to 10°C)'
            WHEN temperature_c BETWEEN 10.01 AND 20 THEN 'Moderate (10°C to 20°C)'
            ELSE 'Warm (>20°C)'
        END AS temp_category,
        CASE
            WHEN wind_speed_ms IS NULL THEN 'Unknown'
            WHEN wind_speed_ms < 2 THEN 'Calm (<2 m/s)'
            WHEN wind_speed_ms BETWEEN 2 AND 6 THEN 'Moderate Wind (2-6 m/s)'
            ELSE 'High Wind (>6 m/s)'
        END AS wind_category
    FROM integrated_taxi_trips
    WHERE trip_distance > 0 AND trip_distance < 100
)
SELECT
    temp_category,
    wind_category,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles
FROM binned_weather
GROUP BY temp_category, wind_category
ORDER BY temp_category, wind_category
"""

QUERY_3_PM25_DEMAND = """
WITH rounded_pm25 AS (
    SELECT
        ROUND(pm25, 0) AS pm25_level,
        pickup_date,
        pickup_hour
    FROM integrated_taxi_trips
    WHERE pm25 IS NOT NULL AND pickup_date IS NOT NULL AND pickup_hour IS NOT NULL
)
SELECT
    pm25_level,
    COUNT(*) AS trips,
    COUNT(DISTINCT struct(pickup_date, pickup_hour)) AS observed_hours,
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT struct(pickup_date, pickup_hour)), 2) AS trips_per_hour
FROM rounded_pm25
GROUP BY pm25_level
ORDER BY pm25_level DESC
"""

QUERY_4_ZONE_WEATHER_SENSITIVITY = """
WITH trips_with_weather AS (
    SELECT
        pickup_zone,
        pickup_date,
        pickup_hour,
        NTILE(4) OVER (
            PARTITION BY pickup_zone
            ORDER BY (10 * sqrt(wind_speed_ms) - wind_speed_ms + 10.5) * (33 - temperature_c)
        ) AS weather_condition
    FROM integrated_taxi_trips
    WHERE pickup_zone IS NOT NULL AND pickup_date IS NOT NULL AND pickup_hour IS NOT NULL
),
hourly_demand AS (
    SELECT
        pickup_zone,
        weather_condition,
        COUNT(1) / COUNT(DISTINCT struct(pickup_date, pickup_hour)) AS trips_per_hour
    FROM trips_with_weather
    GROUP BY pickup_zone, weather_condition
),
pivoted AS (
    SELECT * FROM hourly_demand
    PIVOT (
        ROUND(AVG(trips_per_hour), 2)
        FOR weather_condition IN (1 AS coldest, 2 AS cool, 3 AS warm, 4 AS warmest)
    )
)
SELECT
    pickup_zone,
    coldest, cool, warm, warmest,
    ROUND(((GREATEST(coldest, cool, warm, warmest) - LEAST(coldest, cool, warm, warmest)) / ((coldest + cool + warm + warmest) / 4.0)) * 100, 2) AS pct_variation
FROM pivoted
WHERE (coldest + cool + warm + warmest) / 4.0 >= 10
ORDER BY pct_variation DESC
"""

QUERY_5_PEAK_HOURS_BY_DOW = """
WITH hourly_by_dow AS (
    SELECT
        CASE dayofweek(pickup_date)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_of_week,
        CASE dayofweek(pickup_date)
            WHEN 1 THEN 7
            ELSE dayofweek(pickup_date) - 1
        END AS dow_order,
        pickup_hour,
        COUNT(*) AS trip_count,
        COUNT(DISTINCT pickup_date) AS active_days,
        ROUND(
            CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT pickup_date),
            2
        ) AS avg_trips
    FROM integrated_taxi_trips
    WHERE pickup_date IS NOT NULL
      AND pickup_hour IS NOT NULL
    GROUP BY dayofweek(pickup_date), pickup_hour
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day_of_week
            ORDER BY avg_trips DESC, trip_count DESC
        ) AS peak_rank
    FROM hourly_by_dow
)
SELECT
    day_of_week,
    pickup_hour AS peak_hour,
    trip_count,
    active_days,
    avg_trips
FROM ranked
WHERE peak_rank = 1
ORDER BY dow_order
"""

QUERY_6_MONTHLY_TRENDS = """
WITH monthly AS (
    SELECT
        TRUNC(pickup_date, 'MM') AS trip_month,
        COUNT(*) AS total_trips,
        COUNT(DISTINCT pickup_date) AS active_days,
        ROUND(
            CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT pickup_date),
            2
        ) AS avg_daily_trips
    FROM integrated_taxi_trips
    WHERE pickup_date IS NOT NULL
    GROUP BY TRUNC(pickup_date, 'MM')
)
SELECT
    trip_month,
    total_trips,
    active_days,
    avg_daily_trips,
    LAG(avg_daily_trips) OVER (ORDER BY trip_month) AS prev_month_avg_daily,
    ROUND(
        100.0 * (
            avg_daily_trips - LAG(avg_daily_trips) OVER (ORDER BY trip_month)
        ) / LAG(avg_daily_trips) OVER (ORDER BY trip_month),
        2
    ) AS pct_change_vs_prev_month
FROM monthly
ORDER BY trip_month
"""

# Product builders may need schema-evolved variants (humidity / aqi)
def query_2_with_humidity(has_humidity: bool) -> str:
    humidity_select = (
        "ROUND(AVG(humidity), 2) AS avg_humidity_pct," if has_humidity else ""
    )
    humidity_col = "humidity," if has_humidity else ""
    return f"""
WITH binned_weather AS (
    SELECT
        trip_distance,
        {humidity_col}
        CASE
            WHEN temperature_c IS NULL THEN 'Unknown'
            WHEN temperature_c < 0 THEN 'Freezing (<0°C)'
            WHEN temperature_c BETWEEN 0 AND 10 THEN 'Cold (0°C to 10°C)'
            WHEN temperature_c BETWEEN 10.01 AND 20 THEN 'Moderate (10°C to 20°C)'
            ELSE 'Warm (>20°C)'
        END AS temp_category,
        CASE
            WHEN wind_speed_ms IS NULL THEN 'Unknown'
            WHEN wind_speed_ms < 2 THEN 'Calm (<2 m/s)'
            WHEN wind_speed_ms BETWEEN 2 AND 6 THEN 'Moderate Wind (2-6 m/s)'
            ELSE 'High Wind (>6 m/s)'
        END AS wind_category
    FROM integrated_taxi_trips
    WHERE trip_distance > 0 AND trip_distance < 100
)
SELECT
    temp_category,
    wind_category,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    {humidity_select}
    CURRENT_TIMESTAMP() AS _computed_at
FROM binned_weather
GROUP BY temp_category, wind_category
ORDER BY temp_category, wind_category
"""


def query_3_with_aqi(has_aqi: bool) -> str:
    aqi_select = "ROUND(AVG(aqi), 0) AS avg_aqi_index," if has_aqi else ""
    aqi_col = "aqi," if has_aqi else ""
    return f"""
WITH rounded_pm25 AS (
    SELECT
        ROUND(pm25, 0) AS pm25_level,
        {aqi_col}
        pickup_date,
        pickup_hour
    FROM integrated_taxi_trips
    WHERE pm25 IS NOT NULL AND pickup_date IS NOT NULL AND pickup_hour IS NOT NULL
)
SELECT
    pm25_level,
    {aqi_select}
    COUNT(*) AS trips,
    COUNT(DISTINCT struct(pickup_date, pickup_hour)) AS observed_hours,
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT struct(pickup_date, pickup_hour)), 2) AS trips_per_hour
FROM rounded_pm25
GROUP BY pm25_level
ORDER BY pm25_level DESC
"""


QUERY_DAILY_BOROUGH_MOBILITY = """
SELECT
    pickup_date,
    pickup_borough,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM integrated_taxi_trips
WHERE pickup_borough != 'UNKNOWN' AND pickup_date IS NOT NULL
GROUP BY pickup_date, pickup_borough
"""

QUERIES = {
    "q1_monthly_zone_demand": QUERY_1_MONTHLY_ZONE_DEMAND,
    "q2_weather_distance": QUERY_2_WEATHER_DISTANCE,
    "q3_pm25_demand": QUERY_3_PM25_DEMAND,
    "q4_zone_weather_sensitivity": QUERY_4_ZONE_WEATHER_SENSITIVITY,
    "q5_peak_hours_by_dow": QUERY_5_PEAK_HOURS_BY_DOW,
    "q6_monthly_trends": QUERY_6_MONTHLY_TRENDS,
}
