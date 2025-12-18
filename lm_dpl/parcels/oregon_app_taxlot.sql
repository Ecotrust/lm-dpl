/* 
This query creates the LandMapper taxlot table 
for Django app.models.taxtlot
*/
BEGIN;

-- Create spatial indexes
DROP INDEX IF EXISTS s_oregon_taxlots_post_geom_idx;
CREATE INDEX s_oregon_taxlots_post_geom_idx
    ON s_oregon_taxlots_post
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_taxlots_post_hash_idx;
CREATE INDEX s_oregon_taxlots_post_hash_idx
    ON s_oregon_taxlots_post (id, geohash10);

DROP INDEX IF EXISTS s_oregon_huc_geom_idx;
CREATE INDEX s_oregon_huc_geom_idx
    ON s_oregon_huc
    USING GIST (geom);

DROP INDEX IF EXISTS s_oregon_plss_geom_idx;
CREATE INDEX IF NOT EXISTS s_oregon_plss_geom_idx
    ON s_oregon_plss
    USING GIST (geom);

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

DROP INDEX IF EXISTS s_oregon_elevation_hash;
CREATE INDEX s_oregon_elevation_hash
    ON s_oregon_elevation (id, geohash10);

DROP INDEX IF EXISTS s_oregon_ppa_geom_idx;
CREATE INDEX s_oregon_ppa_geom_idx
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
SELECT t.id, t.county, t.maptaxlot, t.geohash10, e.min_elev, e.max_elev, t.geom
FROM s_oregon_taxlots_post t
JOIN (
	SELECT *
	FROM s_oregon_elevation
	EXCEPT
	SELECT e.*
	FROM (
		SELECT * FROM s_oregon_elevation
		WHERE total_pix > 0 AND 100*forest_pix/total_pix < 20 
	) e
	JOIN s_oregon_taxlots_post t ON t.geohash10 = e.geohash10
	JOIN s_oregon_ppa p ON ST_Intersects(p.geom, t.geom)
) e ON t.geohash10 = e.geohash10
LEFT JOIN oregon_app_taxlot app
ON t.maptaxlot = app.map_taxlot
WHERE
    app.map_id IS DISTINCT FROM t.geohash10 OR
    app.min_elevation IS DISTINCT FROM e.min_elev OR
    app.max_elevation IS DISTINCT FROM e.max_elev;


CREATE INDEX oregon_taxlots_temp_geom
    ON oregon_taxlots_temp 
    USING GIST (geom);

CREATE INDEX oregon_taxlots_temp_hash
    ON oregon_taxlots_temp (geohash10);

-- Main query to join taxlots with various spatial datasets
WITH
-- 1. Watersheds: 
huc_join AS (
    SELECT
        t.geohash10 AS taxlot_id,
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
        t.geohash10 AS taxlot_id,
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
        t.geohash10 AS taxlot_id,
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
        t.geohash10 AS taxlot_id,
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
        t.geohash10 AS taxlot_id,
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
    t.geohash10,
    t.maptaxlot,
    t.geom
FROM oregon_taxlots_temp t
-- JOIN elev_join e ON t.geohash10 = e.geohash10
-- JOIN s_oregon_county_fips_mapping cty ON t.county = cty.county
LEFT JOIN
    (SELECT * FROM huc_join WHERE rn = 1) huc ON t.geohash10 = huc.taxlot_id
LEFT JOIN
    (SELECT * FROM plss_join WHERE rn = 1) plss ON t.geohash10 = plss.taxlot_id
LEFT JOIN
    (SELECT * FROM fpd_join WHERE rn = 1) fpd ON t.geohash10 = fpd.taxlot_id
LEFT JOIN
    (SELECT * FROM sfd_join WHERE rn = 1) sfd ON t.geohash10 = sfd.taxlot_id
LEFT JOIN
    (SELECT * FROM zoning_join WHERE rn = 1) zn ON t.geohash10 = zn.taxlot_id
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
COMMIT;

DROP TABLE IF EXISTS oregon_taxlots_temp;

-- Create indexes on oregon_app_taxlot
CREATE INDEX oregon_app_taxlot_geometry_idx 
    ON public.oregon_app_taxlot 
    USING gist (geometry);
CREATE UNIQUE INDEX oregon_app_taxlot_pkey 
    ON public.oregon_app_taxlot 
    USING btree (id);

-- The End
