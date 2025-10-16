WITH date_series AS (
    SELECT
        addDays(
            toDate('{{ var("start_date", "2014-01-01") }}'),
            number
        ) AS full_date
    FROM
        system.numbers
    LIMIT
        dateDiff(
            'day', toDate('{{ var("start_date", "2014-01-01") }}'), toDate('{{ var("end_date", "2021-12-31") }}')
        ) + 1
)

SELECT
    toInt32(formatDateTime(full_date, '%Y%m%d')) AS date_key,
    full_date,
    toYear(full_date) AS year,
    toQuarter(full_date) AS quarter,
    concat('Q', toString(toQuarter(full_date))) AS quarter_name,
    toMonth(full_date) AS month,
    CASE toMonth(full_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,
    toDayOfMonth(full_date) AS day_of_month,
    toDayOfYear(full_date) AS day_of_year,
    toWeek(full_date, 1) AS week_of_year,
    toDayOfWeek(full_date) AS day_of_week,  -- 1 = Monday, 7 = Sunday
    CASE toDayOfWeek(full_date)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_name,
    toDayOfWeek(full_date) IN (6, 7) AS is_weekend,
    toDayOfMonth(full_date) = 1 AS is_month_start,
    full_date = toLastDayOfMonth(full_date) AS is_month_end

FROM date_series

UNION ALL

SELECT
    toInt32(-1) AS date_key,
    toDate('1900-01-01') AS full_date,
    toInt16(0) AS year,
    toInt8(0) AS quarter,
    toString('Q0') AS quarter_name,
    toInt8(0) AS month,
    toString('Unknown') AS month_name,
    toInt8(0) AS day_of_month,
    toInt16(0) AS day_of_year,
    toInt8(0) AS week_of_year,
    toInt8(0) AS day_of_week,
    toString('Unknown') AS day_of_week_name,
    toBool(0) AS is_weekend,
    toBool(0) AS is_month_start,
    toBool(0) AS is_month_end