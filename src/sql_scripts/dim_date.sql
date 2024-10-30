CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    day_of_year INTEGER,
    week_key INTEGER,
    week_of_year INTEGER,
    day_of_week INTEGER,
    iso_day_of_week INTEGER,
    day_name STRING,
    first_day_of_week DATE,
    last_day_of_week DATE,
    month_key STRING,
    month_of_year INTEGER,
    day_of_month INTEGER,
    month_name_short STRING,
    month_name STRING,
    first_day_of_month DATE,
    last_day_of_month DATE,
    quarter_key INTEGER,
    quarter_of_year INTEGER,
    day_of_quarter INTEGER,
    quarter_desc_short STRING,
    quarter_desc STRING,
    first_day_of_quarter DATE,
    last_day_of_quarter DATE,
    year_key INTEGER,
    first_day_of_year DATE,
    last_day_of_year DATE,
    ordinal_weekday_of_month INTEGER
);

INSERT INTO dim_date (
    date_key, day_of_year, week_key, week_of_year, day_of_week, iso_day_of_week,
    day_name, first_day_of_week, last_day_of_week, month_key, month_of_year, day_of_month,
    month_name_short, month_name, first_day_of_month, last_day_of_month, quarter_key,
    quarter_of_year, day_of_quarter, quarter_desc_short, quarter_desc, first_day_of_quarter,
    last_day_of_quarter, year_key, first_day_of_year, last_day_of_year, ordinal_weekday_of_month
)
WITH generate_date AS (
    SELECT CAST(RANGE AS DATE) AS date_key 
    FROM RANGE(DATE '2014-01-01', DATE '2040-12-31', INTERVAL 1 DAY)
)
SELECT 
    date_key AS date_key,
    DAYOFYEAR(date_key) AS day_of_year,
    YEARWEEK(date_key) AS week_key,
    WEEKOFYEAR(date_key) AS week_of_year,
    DAYOFWEEK(date_key) AS day_of_week,
    ISODOW(date_key) AS iso_day_of_week,
    DAYNAME(date_key) AS day_name,
    DATE_TRUNC('week', date_key) AS first_day_of_week,
    DATE_TRUNC('week', date_key) + 6 AS last_day_of_week,
    YEAR(date_key) || RIGHT('0' || MONTH(date_key), 2) AS month_key,
    MONTH(date_key) AS month_of_year,
    DAYOFMONTH(date_key) AS day_of_month,
    LEFT(MONTHNAME(date_key), 3) AS month_name_short,
    MONTHNAME(date_key) AS month_name,
    DATE_TRUNC('month', date_key) AS first_day_of_month,
    LAST_DAY(date_key) AS last_day_of_month,
    CAST(YEAR(date_key) || QUARTER(date_key) AS INTEGER) AS quarter_key,
    QUARTER(date_key) AS quarter_of_year,
    CAST(date_key - DATE_TRUNC('quarter', date_key) + 1 AS INTEGER) AS day_of_quarter,
    ('Q' || QUARTER(date_key)) AS quarter_desc_short,
    ('Quarter ' || QUARTER(date_key)) AS quarter_desc,
    DATE_TRUNC('quarter', date_key) AS first_day_of_quarter,
    LAST_DAY(DATE_TRUNC('quarter', date_key) + INTERVAL 2 MONTH) AS last_day_of_quarter,
    CAST(YEAR(date_key) AS INTEGER) AS year_key,
    DATE_TRUNC('year', date_key) AS first_day_of_year,
    DATE_TRUNC('year', date_key) - 1 + INTERVAL 1 YEAR AS last_day_of_year,
    ROW_NUMBER() OVER (PARTITION BY YEAR(date_key), MONTH(date_key), DAYOFWEEK(date_key) ORDER BY date_key) AS ordinal_weekday_of_month
FROM generate_date
ORDER BY date_key;
