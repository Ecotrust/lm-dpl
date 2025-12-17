BEGIN;
DROP TABLE IF EXISTS s_washington_taxlots_post;
CREATE TABLE s_washington_taxlots_post (
    id BIGINT PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64),
    landuse_cd SMALLINT,
    area_sqm DOUBLE PRECISION,
    geohash10 VARCHAR(10),
    geom GEOMETRY(MULTIPOLYGON, 3857)
);


WITH duplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY orig_parcel_id 
            ORDER BY id             
        ) AS row_num
    FROM
        s_washington_taxlots
)
INSERT INTO s_washington_taxlots_post
SELECT 
	id,
    county, 
    maptaxlot, 
    landuse_cd,
    area_sqm, 
    geohash10, 
    geom 
FROM (
	SELECT
		id,
	    county_nm as county, 
	    'OID' || CAST(objectid AS VARCHAR) || UPPER(ST_GeoHash(ST_Transform(geom, 4326), 6)) as maptaxlot, 
	    landuse_cd,
	    ST_Area(ST_Transform(geom, 5070)) AS area_sqm, 
	    ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, 
	    geom 
	FROM
	    duplicated
	WHERE
	    row_num > 1
)
UNION
(
	SELECT
		id,
	    county_nm as county, 
	    COALESCE(NULLIF(orig_parcel_id, '0'), CAST(objectid AS VARCHAR)) as maptaxlot, 
	    landuse_cd,
	    ST_Area(ST_Transform(geom, 5070)) AS area_sqm, 
	    ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, 
	    geom 
	FROM duplicated
	WHERE
	    row_num = 1
)
UPDATE s_washington_taxlots
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);
COMMIT;
