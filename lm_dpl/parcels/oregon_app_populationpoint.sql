-- PopulationPoint table
DROP TABLE IF EXISTS app_populationpoint;
CREATE TABLE app_populationpoint (
    id SERIAL PRIMARY KEY,
    classification VARCHAR(100) NOT NULL DEFAULT 'city',
    state VARCHAR(30) NOT NULL DEFAULT 'OR',
    population INTEGER NOT NULL,
    place_fips INTEGER NOT NULL,
    density_sqmi DOUBLE PRECISION DEFAULT NULL,
    area_sqmi DOUBLE PRECISION DEFAULT NULL,
    population_class SMALLINT DEFAULT NULL,
    geometry geometry(POINT, 3857) DEFAULT NULL
);

INSERT INTO app_populationpoint (
    classification,
    state,
    population,
    place_fips,
    density_sqmi,
    area_sqmi,
    population_class,
    geometry
)
SELECT
    class,
    st,
    population,
    CAST(placefips AS INTEGER) AS place_fips,
    pop_sqmi,
    sqmi,
    pop_class,
    ST_Centroid(geom) AS geometry
FROM s_oregon_ppa;
