CREATE TABLE IF NOT EXISTS trips
(
	region VARCHAR,
	origin_coord_x NUMERIC(20, 17),
    origin_coord_y NUMERIC(20, 17),
    destination_coord_x NUMERIC(20, 17),
    destination_coord_y NUMERIC(20, 17),
	datetime TIMESTAMP,
	datasource VARCHAR
);
