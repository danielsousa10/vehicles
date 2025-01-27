
SELECT
    T.box,
    AVG(num_trips) AS avg_weekly_trips
FROM   
    (
        SELECT 
            'BOX({0})' AS box, 
            DATE_PART('year', datetime)::text || DATE_PART('week', datetime)::text AS year_week, 
            COUNT(*) AS num_trips
        FROM trips
        WHERE 
            BOX('{0}') @> POINT(origin_coord_X, origin_coord_y)
        GROUP BY 
            box, 
            year_week
    ) T
GROUP BY
    T.box
ORDER BY avg_weekly_trips DESC;