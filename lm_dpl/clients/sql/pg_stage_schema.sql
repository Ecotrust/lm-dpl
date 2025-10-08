DROP TABLE IF EXISTS s_taxlots_wa;
CREATE TABLE s_taxlots_geom (
    ID BIGSERIAL PRIMARY KEY,
    FIPS_NR TEXT,
    COUNTY_NM TEXT,
    PARCEL_ID_NR TEXT,
    ORIG_PARCEL_ID TEXT,
    LANDUSE_CD TEXT,
    DATA_LINK TEXT,
    FILE_DATE TEXT,
    GEOM GEOMETRY(MultiPolygon, 2927)
);

DROP TABLE IF EXISTS s_ssurgo_geom;
CREATE TABLE s_ssurgo_geom (
    ID BIGSERIAL PRIMARY KEY,
    MUKEY TEXT,
    GEOM GEOMETRY(MultiPolygon, 3857)
);

DROP TABLE IF EXISTS s_ssurgo_data;
CREATE TABLE s_ssurgo_data (
    ID BIGSERIAL PRIMARY KEY,
    MUKEY TEXT,
    MUNAME TEXT,
    DRCLASSDCD TEXT,
    FORPEHRTDCP TEXT,
    SI_LABEL TEXT,
    AVG_RS_L FLOAT,
    AVG_RS_H FLOAT
);

-- DROP INDEX IF EXISTS idx_s_ssurgo_geom;
-- CREATE INDEX idx_s_ssurgo_geom ON s_ssurgo_geom USING GIST (GEOM);
-- DROP INDEX IF EXISTS idx_s_taxlots_geom;
-- CREATE INDEX idx_s_taxlots_geom ON s_taxlots_geom USING GIST (GEOM);
