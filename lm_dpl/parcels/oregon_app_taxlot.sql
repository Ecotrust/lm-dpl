/* 
This query creates the LandMapper taxlot table 
for Django app.models.taxlot
*/
BEGIN;

-- Create spatial indexes
CREATE INDEX IF NOT EXISTS s_oregon_taxlots_post_geom_idx
    ON s_oregon_taxlots_post
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_taxlots_post_hash_idx
    ON s_oregon_taxlots_post (maptaxlot, geohash11);

CREATE INDEX IF NOT EXISTS s_oregon_huc_geom_idx
    ON s_oregon_huc
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_plss_geom_idx
    ON s_oregon_plss
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_fpd_geom_idx
    ON s_oregon_fpd
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_sfd_geom_idx
    ON s_oregon_sfd
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_zoning_geom_idx
    ON s_oregon_zoning
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_elevation_hash
    ON s_oregon_elevation (maptaxlot, geohash11);

CREATE INDEX IF NOT EXISTS s_oregon_ppa_geom_idx
    ON s_oregon_ppa
    USING GIST (geom);

-- Taxlot table
CREATE TABLE IF NOT EXISTS oregon_app_taxlot (
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
    map_taxlot VARCHAR(255) UNIQUE DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

DROP TABLE IF EXISTS oregon_taxlots_temp;
CREATE TABLE oregon_taxlots_temp AS
SELECT t.id, t.county, t.maptaxlot, t.geohash11, e.min_elev, e.max_elev, t.geom
FROM s_oregon_taxlots_post t
JOIN (
	SELECT *
	FROM s_oregon_elevation
	EXCEPT
    -- Exclude small taxlots and taxlots with low forest cover within PPAs
	SELECT e.*
	FROM s_oregon_elevation e
	JOIN s_oregon_taxlots_post t 
		ON t.maptaxlot = e.maptaxlot
	JOIN s_oregon_ppa p
		ON ST_Intersects(t.geom, p.geom)
	WHERE t.area_sqm < 2024
		OR (e.forest_pix/CAST(e.total_pix AS FLOAT) < 0.20) 
) e 
    ON t.maptaxlot = e.maptaxlot
LEFT JOIN oregon_app_taxlot app
    ON t.maptaxlot = app.map_taxlot
WHERE
    app.map_id IS DISTINCT FROM t.geohash11 OR
    app.min_elevation IS DISTINCT FROM e.min_elev OR
    app.max_elevation IS DISTINCT FROM e.max_elev;


CREATE INDEX oregon_taxlots_temp_geom
    ON oregon_taxlots_temp 
    USING GIST (geom);

CREATE INDEX oregon_taxlots_temp_hash
    ON oregon_taxlots_temp (maptaxlot);

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
    FROM oregon_taxlots_temp t
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
    FROM oregon_taxlots_temp t
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
    FROM oregon_taxlots_temp t
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
    FROM oregon_taxlots_temp t
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
    FROM oregon_taxlots_temp t
    JOIN s_oregon_zoning z ON ST_Intersects(t.geom, z.geom)
)
-- Final insert into oregon_taxlots table
INSERT INTO oregon_app_taxlot (
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
    COALESCE(zn.orzdesc, 'Not Defined') AS orzdesc,
    huc.huc12,
    NULL,
    t.min_elev,
    t.max_elev,
    plss.legal_desc,
    t.county,
    'ORMAP',
    t.geohash11,
    t.maptaxlot,
    t.geom
FROM oregon_taxlots_temp t
-- JOIN elev_join e ON t.geohash11 = e.geohash11
-- JOIN s_oregon_county_fips_mapping cty ON t.county = cty.county
LEFT JOIN
    (SELECT * FROM huc_join WHERE rn = 1) huc ON t.id = huc.taxlot_id
LEFT JOIN
    (SELECT * FROM plss_join WHERE rn = 1) plss ON t.id = plss.taxlot_id
LEFT JOIN
    (SELECT * FROM fpd_join WHERE rn = 1) fpd ON t.id = fpd.taxlot_id
LEFT JOIN
    (SELECT * FROM sfd_join WHERE rn = 1) sfd ON t.id = sfd.taxlot_id
LEFT JOIN
    (SELECT * FROM zoning_join WHERE rn = 1) zn ON t.id = zn.taxlot_id
ON CONFLICT (map_taxlot)
DO UPDATE SET
    map_id = EXCLUDED.map_id,
    orzdesc = EXCLUDED.orzdesc,
    min_elevation = EXCLUDED.min_elevation,
    max_elevation = EXCLUDED.max_elevation,
    legal_label = EXCLUDED.legal_label
WHERE
    oregon_app_taxlot.map_id IS DISTINCT FROM EXCLUDED.map_id OR
    oregon_app_taxlot.orzdesc IS DISTINCT FROM EXCLUDED.orzdesc OR
    oregon_app_taxlot.min_elevation IS DISTINCT FROM EXCLUDED.min_elevation OR
    oregon_app_taxlot.max_elevation IS DISTINCT FROM EXCLUDED.max_elevation OR
    oregon_app_taxlot.legal_label IS DISTINCT FROM EXCLUDED.legal_label;

-- Remove taxlots from oregon_app_taxlot that are no longer present in s_oregon_taxlots_post
WITH 
to_delete AS ( 
    SELECT * FROM oregon_app_taxlot
    EXCEPT
    SELECT app.* FROM oregon_app_taxlot app
    INNER JOIN s_oregon_taxlots_post post 
        ON app.map_taxlot = post.maptaxlot
)
DELETE FROM oregon_app_taxlot
WHERE map_taxlot IN (SELECT DISTINCT map_taxlot FROM to_delete);

DROP TABLE IF EXISTS oregon_taxlots_temp;

COMMIT;

-- Create indexes on oregon_app_taxlot
-- CREATE INDEX oregon_app_taxlot_geometry_idx 
--     ON public.oregon_app_taxlot 
--     USING gist (geometry);
-- CREATE UNIQUE INDEX oregon_app_taxlot_pkey 
--     ON public.oregon_app_taxlot 
--     USING btree (id);

-- The End
