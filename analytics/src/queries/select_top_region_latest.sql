WITH top_region AS
(
    SELECT 
        region,
        COUNT(*) AS num_trips
    FROM trips
    GROUP BY
        region
    ORDER by num_trips DESC
    LIMIT {0}
)

SELECT
    T.region,
    T.datasource AS latest_datasource,
    T.datetime
FROM
(
    SELECT 
        TP.region,
        TP.datasource,
        TP.datetime,
        TR.num_trips,
        RANK() OVER (PARTITION BY TP.region ORDER BY datetime DESC) AS _rank
    FROM trips TP
    INNER JOIN top_region TR 
    ON TP.region = TR.region
) T
WHERE T._rank = 1
ORDER BY T.num_trips DESC;

