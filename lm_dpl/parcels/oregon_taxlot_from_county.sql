/* 
 This query creates a unified Oregon taxlot table from individual 
 county taxlot records.

 For counties without taxlot data, taxlot records are extracted from
 the s_oregon_taxlots_old table.

 The script is intended to be run manually after individual county taxlot tables
 have been created. 
 */

-- Current, county code in s_oregon_taxlots_old does not map to fips codes
-- thus we need to create county code to county fips mapping table
CREATE TABLE IF NOT EXISTS s_oregon_county_fips_mapping AS
SELECT t.county, c.county_fipscode, c.county_name 
FROm s_oregon_taxlots_old t
JOIN (
	SELECT DISTINCT ON (county) id
	FROM s_oregon_taxlots_old 
	WHERE county > 0
	GROUP BY county, id
) f ON t.id = f.id
JOIN s_oregon_cty c ON ST_Intersects(t.geom, c.geom)
ORDER BY c.county_fipscode;


DROP TABLE IF EXISTS s_oregon_taxlots_post;
CREATE TABLE s_oregon_taxlots (
    id SERIAL PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64),
    area_sqm DOUBLE PRECISION,
    geohash10 VARCHAR(10),
    geom GEOMETRY(MULTIPOLYGON, 3857)
);

INSERT INTO s_oregon_taxlots_post (county, maptaxlot, area_sqm, geohash10, geom)
SELECT 'Baker' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_baker
UNION 
SELECT 'Benton' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_benton
UNION
SELECT 'Clackamas' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_clackamas
UNION
SELECT 'Clatsop' AS county, taxmapkey AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_clatsop
UNION
SELECT 'Columbia' AS county, map_tax AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_columbia
UNION
SELECT 'Coos' AS county, tlid AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_coos
UNION
SELECT 'Crook' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_crook
UNION
SELECT 'Curry' AS county, currytaxlots_maptaxlot AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_curry
UNION
SELECT 'Deschutes' AS county, taxlot AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_deschutes
UNION
SELECT 'Douglas' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_douglas
UNION
SELECT 'Harney' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_harney
UNION
SELECT 'Hood River' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_hoodriver
UNION
SELECT 'Jackson' AS county, CONCAT(RPAD(TRIM(mapnum), 8, '0'), TRIM(TO_CHAR(taxlot, '00000'))) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_jackson
UNION
SELECT 'Josephine' AS county, mapnum AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_josephine
UNION
SELECT 'Klamath' AS county, SUBSTRING(maptaxlot_, 3, 16) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_klamath
UNION
SELECT 'Lake' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_lake
UNION
SELECT 'Lane' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_lane
UNION
SELECT 'Lincoln' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_lincoln
UNION
SELECT 'Marion' AS county, taxlot AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_marion
UNION
SELECT 'Morrow' AS county, CONCAT(RPAD(TRIM(mapnumber), 10, '0'), TRIM(LPAD(taxlot, 5, '0'))) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_morrow
UNION
SELECT 'Multnomah' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_multnomah
UNION
SELECT 'Polk' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_polk
UNION
SELECT 'Tillamook' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_tillamook
WHERE LENGTH(TRIM(maptaxlot)) > 0
UNION
SELECT 'Umatilla' AS county, map_tax AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_umatilla
UNION
SELECT 'Union' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_union
UNION
SELECT 'Wallowa' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_wallowa
UNION
SELECT 'Wasco' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_wasco
UNION
SELECT 'Washington' AS county, tlno AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_washington
UNION
SELECT 'Wheeler' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_wheeler
UNION
SELECT 'Yamhill' AS county, maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, geom 
FROM s_oregon_taxlots_yamhill
UNION 
SELECT m.county_name as county, t.maptaxlot, ST_Area(ST_Transform(t.geom, 5070)) AS area_sqm, ST_GeoHash(ST_Transform(geom, 4326), 10) AS geohash10, t.geom 
FROM s_oregon_taxlots_old t 
LEFT JOIN s_oregon_county_fips_mapping m
    ON t.county = m.county 
WHERE m.county_name in ('Linn', 'Malheur', 'Jefferson', 'Gilliam', 'Sherman', 'Grant');

-- Fix geoms and remove duplicate records
BEGIN;

UPDATE s_oregon_taxlots_post
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);

WITH duplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY maptaxlot 
            ORDER BY maptaxlot, area_sqm DESC
        ) AS row_num
    FROM
        s_oregon_taxlots_post
)
DELETE FROM s_oregon_taxlots_post t
USING duplicated d
WHERE d.row_num > 1 
    AND t.id = d.id;

COMMIT;
-- END;