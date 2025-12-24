-- PARKED BIKES
CREATE TABLE IF NOT EXISTS bike_parking (
    id SERIAL PRIMARY KEY,
    bike_local_id TEXT NOT NULL REFERENCES bikes(local_id) ON DELETE SET NULL,
    start_time TIMESTAMP,
    end_time   TIMESTAMP,
    latitude   DOUBLE PRECISION NOT NULL,
    longitude  DOUBLE PRECISION NOT NULL,
    place_uid  TEXT NULL REFERENCES places(uid) ON DELETE SET NULL,
    city_uid   TEXT NULL REFERENCES cities(uid) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bike_parking_bike_local_id
    ON bike_parking (bike_local_id);

CREATE INDEX IF NOT EXISTS bike_parking_time_range_gist
    ON bike_parking
    USING GIST (tsrange(start_time, end_time, '[)'));


TRUNCATE TABLE bike_parking RESTART IDENTITY;

SELECT
    bike_local_id,
    COUNT(*) AS occurrences
FROM
    bike_parking
GROUP BY
    bike_local_id
ORDER BY
    occurrences DESC;   -- optional: sorts by most frequent first

-- TRIPS
CREATE TABLE IF NOT EXISTS bike_trips (
    id SERIAL PRIMARY KEY,
    bike_local_id TEXT NOT NULL REFERENCES bikes(local_id) ON DELETE SET NULL,
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    start_latitude  DOUBLE PRECISION NOT NULL,
    start_longitude DOUBLE PRECISION NOT NULL,
    end_latitude    DOUBLE PRECISION NOT NULL,
    end_longitude   DOUBLE PRECISION NOT NULL,
    start_place_uid TEXT NULL REFERENCES places(uid) ON DELETE SET NULL,
    start_city_uid  TEXT NULL REFERENCES cities(uid) ON DELETE SET NULL,
    end_place_uid   TEXT NULL REFERENCES places(uid) ON DELETE SET NULL,
    end_city_uid    TEXT NULL REFERENCES cities(uid) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bike_trips_bike_local_id
    ON bike_trips (bike_local_id);
CREATE INDEX IF NOT EXISTS idx_bike_trips_start_place_uid
    ON bike_trips (start_place_uid);
CREATE INDEX IF NOT EXISTS idx_bike_trips_end_place_uid
    ON bike_trips (end_place_uid);

CREATE INDEX IF NOT EXISTS bike_trips_time_range_gist
    ON bike_trips
    USING GIST (tsrange(start_time, end_time, '[)'));

TRUNCATE TABLE bike_trips RESTART IDENTITY;



INSERT INTO bike_trips (
    bike_local_id,
    start_time,
    end_time,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    start_place_uid,
    end_place_uid,
    start_city_uid,
    end_city_uid
)
WITH ordered AS (
    SELECT
        bike_local_id,
        start_time,
        end_time,
        latitude,
        longitude,
        place_uid,
        city_uid,
        LEAD(start_time)  OVER (PARTITION BY bike_local_id ORDER BY start_time) AS next_start_time,
        LEAD(latitude)    OVER (PARTITION BY bike_local_id ORDER BY start_time) AS next_latitude,
        LEAD(longitude)   OVER (PARTITION BY bike_local_id ORDER BY start_time) AS next_longitude,
        LEAD(place_uid)   OVER (PARTITION BY bike_local_id ORDER BY start_time) AS next_place_uid,
        LEAD(city_uid)    OVER (PARTITION BY bike_local_id ORDER BY start_time) AS next_city_uid
    FROM bike_parking
    WHERE end_time IS NOT NULL
    -- AND city_uid = ''
)
SELECT
    bike_local_id::text,
    end_time AS start_time,
    next_start_time AS end_time,
    latitude  AS start_latitude,
    longitude AS start_longitude,
    next_latitude  AS end_latitude,
    next_longitude AS end_longitude,
    place_uid AS start_place_uid,
    next_place_uid AS end_place_uid,
    city_uid AS start_city_uid,
    next_city_uid AS end_city_uid
FROM ordered
WHERE next_start_time IS NOT NULL
  AND next_start_time > end_time          -- only gaps (actual trips)
  AND next_latitude IS NOT NULL
  AND next_longitude IS NOT NULL;
