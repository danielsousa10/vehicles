
SELECT
    T.region,
    AVG(num_trips) AS avg_weekly_trips
FROM   
    (
        SELECT 
            region, 
            DATE_PART('year', datetime)::text || DATE_PART('week', datetime)::text AS year_week, 
            COUNT(*) AS num_trips
        FROM trips
        WHERE 
            region = '{0}'
            OR '{0}' = ''
        GROUP BY 
            region, 
            year_week
    ) T
GROUP BY
    T.region
ORDER BY avg_weekly_trips DESC;