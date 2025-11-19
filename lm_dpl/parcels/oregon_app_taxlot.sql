/* 
This query creates the LandMapper taxlot table 
for Django app.models.taxtlot
*/

-- Fix invalid geometries
UPDATE s_oregon_sfd
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);

UPDATE s_oregon_zoning
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);


-- Create spatial indexes
DROP INDEX IF EXISTS s_oregon_huc_geom_idx;
CREATE INDEX s_oregon_huc_geom_idx
    ON s_oregon_huc
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_plss1_geom_idx;
CREATE INDEX s_oregon_plss1_geom_idx
    ON s_oregon_plss1
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_plss2_geom_idx;
CREATE INDEX s_oregon_plss2_geom_idx
  ON s_oregon_plss2
  USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_taxlots_geom_idx;
CREATE INDEX s_oregon_taxlots_geom_idx
    ON s_oregon_taxlots
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_taxlots_idx;
CREATE INDEX s_oregon_taxlots_idx
    ON s_oregon_taxlots (id);

DROP INDEX IF EXISTS s_oregon_fpd_geom_idx;
CREATE INDEX s_oregon_fpd_geom_idx
    ON s_oregon_fpd
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_sfd_geom_idx;
CREATE INDEX s_oregon_sfd_geom_idx
    ON s_oregon_sfd
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_zoning_geom_idx;
CREATE INDEX s_oregon_zoning_geom_idx
    ON s_oregon_zoning
    USING GIST (geom);


BEGIN;
-- Create/re-create table to hold the intersected PLSS tables
-- Drop index to reduce overhead on insert.
DROP INDEX IF EXISTS s_oregon_plss_geom_idx;
DROP TABLE IF EXISTS s_oregon_plss;
CREATE TABLE s_oregon_plss (
    id BIGSERIAL PRIMARY KEY,
    legal_desc VARCHAR(64),
    geom GEOMETRY(GEOMETRY, 3857)
);

INSERT INTO s_oregon_plss (legal_desc, geom)
SELECT
    CONCAT('S', frstdivno, ' (', twnshplab, ')') AS legal_desc,
    ST_Intersection(a.geom, b.geom) AS geom
FROM s_oregon_plss1 a
JOIN s_oregon_plss2 b 
    ON ST_Intersects(a.geom, b.geom)
WHERE
    ST_Intersects(a.geom, b.geom) AND 
    ST_Dimension(ST_Intersection(a.geom, b.geom)) = 2;

CREATE INDEX IF NOT EXISTS s_oregon_plss_geom_idx
    ON s_oregon_plss
    USING GIST (geom);


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
    FROM s_oregon_taxlots t
    JOIN s_oregon_huc w ON ST_Intersects(t.geom, w.geom)
),
-- 2. PLSS: 
plss_join AS (
    SELECT
        t.id AS taxlot_id,
        p.legal_desc,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, p.geom)) DESC
        ) as rn
    FROM s_oregon_taxlots t
    JOIN s_oregon_plss p ON ST_Intersects(t.geom, p.geom)
),
-- 3. Forest Protection Districts: 
fpd_join AS (
    SELECT
        t.id AS taxlot_id,
        f.odf_fpd,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, f.geom)) DESC
        ) as rn
    FROM s_oregon_taxlots t
    JOIN s_oregon_fpd f ON ST_Intersects(t.geom, f.geom)
),
-- 4. Structural Fire Districts: 
sfd_join AS (
    SELECT
        t.id AS taxlot_id,
        s.agency_name AS agency,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, s.geom)) DESC
        ) as rn
    FROM s_oregon_taxlots t
    JOIN s_oregon_sfd s ON ST_Intersects(t.geom, s.geom)
),
-- 5. Zoning
zoning_join AS (
    SELECT
        t.id AS taxlot_id,
        z.orzdesc,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, z.geom)) DESC
        ) as rn
    FROM s_oregon_taxlots t
    JOIN s_oregon_zoning z ON ST_Intersects(t.geom, z.geom)
)
-- Final insert into oregon_taxlots table
INSERT INTO app_taxlot (
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
    sfd.agency,
    zn.orzdesc,
    huc.huc12,
    t.ortaxlot,
    NULL,
    NULL,
    plss.legal_desc,
    cty.county_name,
    'ORMAP',
    t.objectid,
    t.maptaxlot,
    t.geom
FROM s_oregon_taxlots t
JOIN s_oregon_cty cty ON t.county = CAST(cty.county_fipscode AS INTEGER)
LEFT JOIN
    (SELECT * FROM huc_join WHERE rn = 1) huc ON t.id = huc.taxlot_id
LEFT JOIN
    (SELECT * FROM plss_join WHERE rn = 1) plss ON t.id = plss.taxlot_id
LEFT JOIN
    (SELECT * FROM fpd_join WHERE rn = 1) fpd ON t.id = fpd.taxlot_id
LEFT JOIN
    (SELECT * FROM sfd_join WHERE rn = 1) sfd ON t.id = sfd.taxlot_id
LEFT JOIN
    (SELECT * FROM zoning_join WHERE rn = 1) zn ON t.id = zn.taxlot_id;
ROLLBACK;

-- Create indexes on app_taxlot
DROP INDEX IF EXISTS app_taxlot_centroid_idx;
CREATE INDEX app_taxlot_centroid_idx 
    ON public.app_taxlot 
    USING gist (centroid);
DROP INDEX IF EXISTS app_taxlot_geometry_idx;
CREATE INDEX app_taxlot_geometry_idx 
    ON public.app_taxlot 
    USING gist (geometry);
DROP INDEX IF EXISTS app_taxlot_pkey;
CREATE UNIQUE INDEX app_taxlot_pkey 
    ON public.app_taxlot 
    USING btree (id);

-- The End