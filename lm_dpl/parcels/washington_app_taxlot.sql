/* 
This query creates the LandMapper taxlot table 
for Django app.models.taxtlot
*/

-- Fix invalid geometries
UPDATE s_washington_fpd
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);

UPDATE s_washington_plss
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);

UPDATE s_washington_taxlots
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);

-- Create spatial indexes
DROP INDEX IF EXISTS s_washington_huc_geom_idx;
CREATE INDEX s_washington_huc_geom_idx
    ON s_washington_huc
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_plss_geom_idx;
CREATE INDEX s_washington_plss_geom_idx
    ON s_washington_plss
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_taxlots_geom_idx;
CREATE INDEX s_washington_taxlots_geom_idx
    ON s_washington_taxlots
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_taxlots_idx;
CREATE INDEX s_washington_taxlots_idx
    ON s_washington_taxlots (id);

DROP INDEX IF EXISTS s_washington_fpd_geom_idx;
CREATE INDEX s_washington_fpd_geom_idx
    ON s_washington_fpd
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_elevation_idx;
CREATE INDEX s_washington_elevation_idx
    ON s_washington_elevation (id);

BEGIN;

DROP INDEX IF EXISTS s_washington_plss_tshp_geom_idx;
DROP TABLE IF EXISTS s_washington_plss_tshp;
CREATE TABLE s_washington_plss_tshp AS
    SELECT *
    FROM s_washington_plss p
    WHERE ST_Area(p.geom) < 10000000;

CREATE INDEX s_washington_plss_tshp_geom_idx
    ON s_washington_plss_tshp
    USING GIST (geom);

-- Taxlot table
DROP TABLE IF EXISTS washington_app_taxlot;
CREATE TABLE washington_app_taxlot (
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


-- Main query to join taxlots with various spatial datasets
WITH
-- 1. Watersheds: 
huc_join AS (
    SELECT
        t.id AS taxlot_id,
        w.name AS watershed_name,
        w.huc12,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, w.geom)) DESC
        ) as rn
    FROM s_washington_taxlots t
    JOIN s_washington_huc w ON ST_Intersects(t.geom, w.geom)
),
-- 2. PLSS: 
plss_join AS (
    SELECT
        t.id AS taxlot_id,
        p.legal_desc_nm AS legal_desc,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, p.geom)) DESC
        ) as rn
    FROM s_washington_taxlots t
    JOIN s_washington_plss_tshp p ON ST_Intersects(t.geom, p.geom)
),
-- 3. Fire Protection Districts: 
fpd_join AS (
    SELECT
        t.id AS taxlot_id,
        LEFT(REPLACE(f.fpd_desc, 'COUNTY', ''), 25) AS odf_fpd,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, f.geom)) DESC
        ) as rn
    FROM s_washington_taxlots t
    JOIN s_washington_fpd f ON ST_Intersects(t.geom, f.geom)
),
-- 4. Elevation:
elev_join AS (
    SELECT *
    FROM s_washington_elevation
    EXCEPT
    SELECT e.* 
    FROM (
        SELECT * FROM s_washington_elevation
        WHERE forest_pix/total_pix < 0.2
    ) e
    JOIN s_washington_taxlots t ON t.id = e.id
    JOIN s_washington_ppa p ON ST_Intersects(p.geom, t.geom)
)
-- Final insert into oregon_taxlots table
INSERT INTO washington_app_taxlot (
    odf_fpd,
    agency,
    orzdesc,
    huc12,
    name,
    min_elevation,
    max_elevation,
    legal_label,
    county,
    source,
    map_id,
    map_taxlot,
    geometry
)
SELECT
    fpd.odf_fpd,
    NULL,
    lu.descr,
    huc.huc12,
    NULL,
    e.min_elev,
    e.max_elev,
    plss.legal_desc,
    t.county_nm,
    'WADNR',
    t.objectid,
    t.orig_parcel_id,
    t.geom
FROM s_washington_taxlots t
JOIN s_washington_cty cty ON t.fips_nr = CAST(cty.county_fipscode AS INTEGER)
JOIN 
    (SELECT DISTINCT landuse_cd,descr FROM public.s_washington_landuse) lu ON t.landuse_cd = lu.landuse_cd
LEFT JOIN
    (SELECT * FROM huc_join WHERE rn = 1) huc ON t.id = huc.taxlot_id
LEFT JOIN
    (SELECT * FROM plss_join WHERE rn = 1) plss ON t.id = plss.taxlot_id
LEFT JOIN
    (SELECT * FROM fpd_join WHERE rn = 1) fpd ON t.id = fpd.taxlot_id
JOIN elev_join e ON t.id = e.id;
COMMIT;

-- Create indexes on washington_app_taxlot
DROP INDEX IF EXISTS washington_app_taxlot_centroid_idx;
CREATE INDEX washington_app_taxlot_centroid_idx 
    ON public.washington_app_taxlot 
    USING gist (centroid);
DROP INDEX IF EXISTS washington_app_taxlot_geometry_idx;
CREATE INDEX washington_app_taxlot_geometry_idx 
    ON public.washington_app_taxlot 
    USING gist (geometry);
DROP INDEX IF EXISTS washington_app_taxlot_pkey;
CREATE UNIQUE INDEX washington_app_taxlot_pkey 
    ON public.washington_app_taxlot 
    USING btree (id);

-- The End