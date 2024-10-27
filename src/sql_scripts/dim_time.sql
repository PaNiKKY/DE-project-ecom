CREATE TABLE dim_time (
    time_key TIME PRIMARY KEY,
    hour_24 INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    second INTEGER NOT NULL,
    period VARCHAR(2),                -- "AM" or "PM"
    is_morning BOOLEAN,
    is_afternoon BOOLEAN,
    is_evening BOOLEAN,
    is_night BOOLEAN
);

-- Insert data into the time table
INSERT INTO dim_time (
    time_key,
    hour_24,
    minute,
    second,
    period,
    is_morning,
    is_afternoon,
    is_evening,
    is_night
)

WITH generate_time AS (
    SELECT CAST(RANGE AS TIME) AS time_key 
    FROM RANGE(TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-02 00:00:00', INTERVAL '1 second')
)

SELECT 
    time_key,
    date_part('hour', time_key) AS hour,
    date_part('minute', time_key) AS minute,
    date_part('second', time_key) AS second,
    CASE WHEN date_part('hour', time_key) < 12 THEN 'AM' ELSE 'PM' END AS period,
    CASE WHEN date_part('hour', time_key) >= 6 AND date_part('hour', time_key) < 12 THEN TRUE ELSE FALSE END AS is_morning,
    CASE WHEN date_part('hour', time_key) >= 12 AND date_part('hour', time_key) < 18 THEN TRUE ELSE FALSE END AS is_afternoon,
    CASE WHEN date_part('hour', time_key) >= 18 AND date_part('hour', time_key) < 21 THEN TRUE ELSE FALSE END AS is_evening,
    CASE WHEN (date_part('hour', time_key) >= 21 AND date_part('hour', time_key) < 24) 
              OR (date_part('hour', time_key) >= 0 AND date_part('hour', time_key) < 6) THEN TRUE ELSE FALSE END AS is_night
FROM 
    generate_time