-- Django models schema 

-- Taxlot table
DROP TABLE IF EXISTS app_taxlot;
CREATE TABLE app_taxlot (
    id SERIAL PRIMARY KEY,
    odf_fpd VARCHAR(25) DEFAULT NULL,
    agency VARCHAR(100) DEFAULT NULL,
    orzdesc VARCHAR(255) DEFAULT NULL,
    huc12 VARCHAR(12) DEFAULT NULL,
    name VARCHAR(120) DEFAULT NULL,
    min_elevation DOUBLE PRECISION DEFAULT NULL,
    max_elevation DOUBLE PRECISION DEFAULT NULL,
    legal_label VARCHAR(255) DEFAULT NULL,
    county VARCHAR(255) DEFAULT NULL,
    source VARCHAR(255) DEFAULT NULL,
    map_id VARCHAR(255) DEFAULT NULL,
    map_taxlot VARCHAR(255) DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

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

-- COA table
DROP TABLE IF EXISTS app_coa;
CREATE TABLE app_coa (
    id SERIAL PRIMARY KEY,
    huc12 VARCHAR(24) NOT NULL,
    coa_name VARCHAR(255) DEFAULT NULL,
    coa_id VARCHAR(6) DEFAULT NULL,
    ecoregion VARCHAR(255) DEFAULT NULL,
    profile_link VARCHAR(255) DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

-- The End