DROP TABLE IF EXISTS s_oregon_taxlots_fcty;
CREATE TABLE s_oregon_taxlots_fcty (
    id SERIAL PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64),
    taxlot_area DOUBLE PRECISION,
    geom GEOMETRY(MULTIPOLYGON, 3857)
);

INSERT INTO s_oregon_taxlots_fcty (county, maptaxlot, taxlot_area, geom)
SELECT 'Baker' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_baker
UNION 
SELECT 'Benton' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_benton
UNION
SELECT 'Clackamas' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_clackamas
UNION
SELECT 'Clatsop' AS county, taxmapkey AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_clatsop
UNION
SELECT 'Columbia' AS county, map_tax AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_columbia
UNION
SELECT 'Coos' AS county, tlid AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_coos
UNION
SELECT 'Crook' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_crook
UNION
SELECT 'Curry' AS county, currytaxlots_maptaxlot AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_curry
UNION
SELECT 'Deschutes' AS county, taxlot AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_deschutes
UNION
SELECT 'Douglas' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_douglas
UNION
SELECT 'Harney' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_harney
UNION
SELECT 'Hood River' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_hoodriver
UNION
SELECT 'Jackson' AS county, CONCAT(RPAD(TRIM(mapnum), 8, '0'), TRIM(TO_CHAR(taxlot, '00000'))) AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_jackson
UNION
SELECT 'Josephine' AS county, mapnum AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_josephine
UNION
SELECT 'Klamath' AS county, SUBSTRING(maptaxlot_, 3, 16) AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_klamath
UNION
SELECT 'Lake' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_lake
UNION
SELECT 'Lane' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_lane
UNION
SELECT 'Lincoln' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_lincoln
UNION
SELECT 'Marion' AS county, taxlot AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_marion
UNION
SELECT 'Morrow' AS county, CONCAT(RPAD(TRIM(mapnumber), 10, '0'), TRIM(LPAD(taxlot, 5, '0'))) AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_morrow
UNION
SELECT 'Multnomah' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_multnomah
UNION
SELECT 'Polk' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_polk
UNION
SELECT 'Tillamook' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_tillamook
UNION
SELECT 'Umatilla' AS county, map_tax AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_umatilla
UNION
SELECT 'Union' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_union
UNION
SELECT 'Wallowa' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_wallowa
UNION
SELECT 'Wasco' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_wasco
UNION
SELECT 'Washington' AS county, tlno AS maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_washington
UNION
SELECT 'Wheeler' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_wheeler
UNION
SELECT 'Yamhill' AS county, maptaxlot, ST_Area(ST_Transform(geom, 2992)) AS taxlot_area, geom 
FROM s_oregon_taxlots_yamhill;
