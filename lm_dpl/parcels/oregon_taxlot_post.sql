DROP TABLE IF EXISTS s_oregon_taxlots_post;
CREATE TABLE s_oregon_taxlots_post (
    id SERIAL PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64),
    area_sqm DOUBLE PRECISION,
    geohash10 VARCHAR(10),
    geom GEOMETRY(MULTIPOLYGON, 3857)
);

INSERT INTO s_oregon_taxlots (county, maptaxlot, area_sqm, geohash10, geom)
SELECT county_name as county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots t 

CREATE INDEX s_oregon_taxlots_post_geom_idx
    ON s_oregon_taxlots_post
    USING GIST (geom);

CREATE INDEX s_oregon_taxlots_post_hash_idx
    ON s_oregon_taxlots_post (geohash10);
