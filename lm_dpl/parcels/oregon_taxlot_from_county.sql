/* 
 This query creates the table `s_oregon_taxlots` from individual 
 county taxlot records.

 For counties without taxlot data, taxlot records are extracted from
 the s_oregon_taxlots_old table.

 The script is intended to be run manually after individual county taxlot tables
 have been created. 
*/

BEGIN;
-- County code in s_oregon_taxlots_old does not map to fips codes
-- thus we need to map county code to county fips.
CREATE TABLE IF NOT EXISTS s_oregon_county_fips_mapping AS
SELECT DISTINCT t.county, c.county_fipscode, c.county_name 
FROM s_oregon_taxlots_old t
JOIN (
    -- Select one ID per county. 
    -- Avoid selecting 'geom' here to keep the sort operation lightweight.
    SELECT DISTINCT ON (county) id
    FROM s_oregon_taxlots_old 
    -- WHERE county > 0 -- Uncomment if filtering 0 is desired
    ORDER BY county, id
) f ON t.id = f.id
JOIN s_oregon_cty c ON ST_Intersects(ST_Centroid(t.geom), c.geom)
ORDER BY c.county_fipscode;


DROP TABLE IF EXISTS s_oregon_taxlots;
CREATE TABLE  s_oregon_taxlots AS
SELECT 'Baker' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_baker
UNION 
SELECT 'Benton' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_benton
UNION
SELECT 'Clackamas' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_clackamas
UNION
SELECT 'Clatsop' AS county, TRIM(taxmapkey) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_clatsop
UNION
SELECT 'Columbia' AS county, TRIM(map_tax) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_columbia
UNION
SELECT 'Coos' AS county, TRIM(tlid) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_coos
UNION
SELECT 'Crook' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_crook
UNION
SELECT 'Curry' AS county, TRIM(currytaxlots_maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_curry
UNION
SELECT 'Deschutes' AS county, TRIM(taxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_deschutes
UNION
SELECT 'Douglas' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_douglas
UNION
SELECT 'Harney' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_harney
UNION
SELECT 'Hood River' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_hoodriver
UNION
SELECT 'Jackson' AS county, CONCAT(RPAD(TRIM(mapnum), 8, '0'), TRIM(TO_CHAR(taxlot, '00000'))) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_jackson
UNION
SELECT 'Josephine' AS county, TRIM(mapnum) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_josephine
UNION
SELECT 'Klamath' AS county, SUBSTRING(maptaxlot_, 3, 16) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_klamath
UNION
SELECT 'Lake' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_lake
UNION
SELECT 'Lane' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_lane
UNION
SELECT 'Lincoln' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_lincoln
UNION
SELECT 'Marion' AS county, TRIM(taxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_marion
UNION
SELECT 'Morrow' AS county, CONCAT(RPAD(TRIM(mapnumber), 10, '0'), TRIM(LPAD(taxlot, 5, '0'))) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_morrow
UNION
SELECT 'Multnomah' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_multnomah
UNION
SELECT 'Polk' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_polk
UNION
SELECT 'Tillamook' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_tillamook
UNION
SELECT 'Umatilla' AS county, TRIM(map_tax) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_umatilla
UNION
SELECT 'Union' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_union
UNION
SELECT 'Wallowa' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_wallowa
UNION
SELECT 'Wasco' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_wasco
UNION
SELECT 'Washington' AS county, TRIM(tlno) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_washington
UNION
SELECT 'Wheeler' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_wheeler
UNION
SELECT 'Yamhill' AS county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots_yamhill
UNION 
SELECT m.county_name as county, TRIM(t.maptaxlot) AS maptaxlot, ST_Area(ST_Transform(t.geom, 5070)) AS area_sqm, t.geom 
FROM s_oregon_taxlots_old t 
LEFT JOIN s_oregon_county_fips_mapping m
    ON t.county = m.county 
    WHERE m.county_name in ('Linn', 'Malheur', 'Jefferson', 'Gilliam', 'Sherman', 'Grant');
-- ) AS all_taxlots
-- WHERE NULLIF(maptaxlot, ' ') IS NOT NULL
--     AND maptaxlot !~* '(ROAD|WATER|RAIL|NONTL|CANAL|RIVER|GAP|RR|WTR|STR|ISLAND|R\/R)';

ALTER TABLE s_oregon_taxlots 
ADD COLUMN id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

ALTER TABLE s_oregon_taxlots
ADD COLUMN county_fips VARCHAR(3);

UPDATE s_oregon_taxlots t
SET county_fips = m.county_fipscode
FROM s_oregon_county_fips_mapping m
WHERE t.county = m.county_name;

COMMIT;

