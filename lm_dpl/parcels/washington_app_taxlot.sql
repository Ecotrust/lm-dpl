/* 
This query creates the LandMapper taxlot table 
for Django app.models.taxtlot
*/

BEGIN;
-- Create spatial indexes
DROP INDEX IF EXISTS s_washington_huc_geom_idx;
CREATE INDEX s_washington_huc_geom_idx
    ON s_washington_huc
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_plss_geom_idx;
CREATE INDEX s_washington_plss_geom_idx
    ON s_washington_plss
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_taxlots_post_geom_idx;
CREATE INDEX s_washington_taxlots_post_geom_idx
    ON s_washington_taxlots_post
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_taxlots_post_hash;
CREATE INDEX s_washington_taxlots_post_hash
    ON s_washington_taxlots_post (geohash11);

DROP INDEX IF EXISTS s_washington_fpd_geom_idx;
CREATE INDEX s_washington_fpd_geom_idx
    ON s_washington_fpd
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_plss_;
CREATE INDEX IF NOT EXISTS s_washington_plss_geom_idx
    ON s_washington_plss
    USING GIST (geom);

DROP INDEX IF EXISTS s_washington_elevation_hash;
CREATE INDEX s_washington_elevation_hash
    ON s_washington_elevation (geohash11);

CREATE TABLE IF NOT EXISTS washington_app_taxlot (
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
    map_id VARCHAR(255) UNIQUE DEFAULT NULL,
    map_taxlot VARCHAR(255) UNIQUE DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

DROP TABLE IF EXISTS washington_taxlots_temp;
CREATE TABLE washington_taxlots_temp AS
SELECT t.id, t.county, t.maptaxlot, t.landuse_cd, t.geohash11, e.min_elev, e.max_elev, t.geom
FROM s_washington_taxlots_post t
JOIN (
	SELECT *
	FROM s_washington_elevation
	EXCEPT
    -- Exclude small taxlots and taxlots with low forest cover within PPAs
	SELECT e.*
	FROM s_washington_elevation e
	JOIN s_washington_taxlots_post t 
		ON t.maptaxlot = e.maptaxlot
	JOIN s_washington_ppa p
		ON ST_Intersects(t.geom, p.geom)
	WHERE t.area_sqm < 2024
		OR (e.forest_pix/CAST(e.total_pix AS FLOAT) < 0.20) 
) e 
    ON t.maptaxlot = e.maptaxlot
LEFT JOIN washington_app_taxlot app
    ON t.maptaxlot = app.map_taxlot
WHERE
    app.map_id IS DISTINCT FROM t.geohash11 OR
    app.min_elevation IS DISTINCT FROM e.min_elev OR
    app.max_elevation IS DISTINCT FROM e.max_elev;

CREATE INDEX washington_taxlots_temp_geom
    ON washington_taxlots_temp 
    USING GIST (geom);

CREATE INDEX washington_taxlots_temp_hash
    ON washington_taxlots_temp (maptaxlot);

-- DROP INDEX IF EXISTS washington_app_taxlot_geometry_idx;
-- DROP INDEX IF EXISTS washington_app_taxlot_pkey;

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
    FROM washington_taxlots_temp t
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
    FROM washington_taxlots_temp t
    JOIN s_washington_plss_tshp p ON ST_Intersects(t.geom, p.geom)
),
-- 3. Fire Protection Districts: 
fpd_join AS (
    SELECT
        t.id AS taxlot_id,
        -- truncating to match landmapper django model schema
        LEFT(REPLACE(f.fpd_desc, 'COUNTY', ''), 25) AS odf_fpd,
        ROW_NUMBER() OVER (
            PARTITION BY t.id
            ORDER BY ST_Area(ST_Intersection(t.geom, f.geom)) DESC
        ) as rn
    FROM washington_taxlots_temp t
    JOIN s_washington_fpd f ON ST_Intersects(t.geom, f.geom)
)
-- Final insert into app_taxlots table
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
    COALESCE(lu.descr, 'Not Defined') AS orzdesc,
    huc.huc12,
    NULL,
    t.min_elev,
    t.max_elev,
    plss.legal_desc,
    t.county,
    'WaTech and Washington State Counties',
    t.geohash11,
    t.maptaxlot,
    t.geom
FROM washington_taxlots_temp t
LEFT JOIN 
    (SELECT DISTINCT landuse_cd,descr FROM public.s_washington_landuse) lu ON t.landuse_cd = lu.landuse_cd
LEFT JOIN
    (SELECT * FROM huc_join WHERE rn = 1) huc ON t.id = huc.taxlot_id
LEFT JOIN
    (SELECT * FROM plss_join WHERE rn = 1) plss ON t.id = plss.taxlot_id
LEFT JOIN
    (SELECT * FROM fpd_join WHERE rn = 1) fpd ON t.id = fpd.taxlot_id
ON CONFLICT (map_taxlot)
DO UPDATE SET
    map_id = EXCLUDED.map_id,
    orzdesc = EXCLUDED.orzdesc,
    min_elevation = EXCLUDED.min_elevation,
    max_elevation = EXCLUDED.max_elevation,
    legal_label = EXCLUDED.legal_label
WHERE
    washington_app_taxlot.map_id IS DISTINCT FROM EXCLUDED.map_id OR
    washington_app_taxlot.orzdesc IS DISTINCT FROM EXCLUDED.orzdesc OR
    washington_app_taxlot.min_elevation IS DISTINCT FROM EXCLUDED.min_elevation OR
    washington_app_taxlot.max_elevation IS DISTINCT FROM EXCLUDED.max_elevation OR
    washington_app_taxlot.legal_label IS DISTINCT FROM EXCLUDED.legal_label;


-- Remove taxlots from washington_app_taxlot that are no longer present in s_washington_taxlots_post
WITH 
to_delete AS ( 
    SELECT * FROM washington_app_taxlot
    EXCEPT
    SELECT app.* FROM washington_app_taxlot app
    INNER JOIN s_washington_taxlots_post post 
        ON app.map_taxlot = post.maptaxlot
)
DELETE FROM washington_app_taxlot
WHERE map_taxlot IN (SELECT DISTINCT map_taxlot FROM to_delete);

DROP TABLE IF EXISTS washington_taxlots_temp;

COMMIT;

-- Create indexes on washington_app_taxlot
-- CREATE INDEX washington_app_taxlot_geometry_idx 
--     ON public.washington_app_taxlot 
--     USING gist (geometry);
-- CREATE UNIQUE INDEX washington_app_taxlot_pkey 
--     ON public.washington_app_taxlot 
--     USING btree (id);
-- The End
