-- create COUNTRIES table
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    local_id TEXT NOT NULL UNIQUE,
    timezone TEXT NOT NULL,
    country TEXT,
    country_name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL
);

-- create CITIES table
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    country_id INTEGER NOT NULL,
    uid TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    alias TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    lat_min DOUBLE PRECISION NOT NULL,
    lng_min DOUBLE PRECISION NOT NULL,
    lat_max DOUBLE PRECISION NOT NULL,
    lng_max DOUBLE PRECISION NOT NULL,

    CONSTRAINT fk_cities_country
        FOREIGN KEY (country_id)
        REFERENCES countries(id)
        ON DELETE RESTRICT
);

-- create PLACES table
CREATE TABLE IF NOT EXISTS places (
    id SERIAL PRIMARY KEY,
    uid TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS city_places (
    city_id  INTEGER NOT NULL REFERENCES cities(id) ON DELETE CASCADE,
    place_id INTEGER NOT NULL REFERENCES places(id) ON DELETE CASCADE,
    PRIMARY KEY (city_id, place_id)
);

-- optional (helps reverse lookups)
CREATE INDEX IF NOT EXISTS city_places_place_id_idx ON city_places(place_id);


-- create BIKES table
CREATE TABLE IF NOT EXISTS bikes (
    id SERIAL PRIMARY KEY,
    local_id TEXT NOT NULL UNIQUE,
    bike_type TEXT NOT NULL
);


-- Rückabwicklung:

TRUNCATE TABLE city_places RESTART IDENTITY;
TRUNCATE TABLE bikes RESTART IDENTITY CASCADE ;
TRUNCATE TABLE cities RESTART IDENTITY CASCADE ;
TRUNCATE TABLE countries RESTART IDENTITY CASCADE ;
TRUNCATE TABLE places RESTART IDENTITY CASCADE;
