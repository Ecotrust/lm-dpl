/* 
 This script performs the following steps:

 Cleaning and filtering

 - Removes records with NULL or empty maptaxlot values
 - Filters out taxlots with codes with invalid numeric patterns except ALLOTTED, ONF, 
   TRIBAL, DNR, BML, STATE, WNF, ONP, IS, RES.
 - Filters out taxlots based on landuse_cd and shape metrics indicating non-parcel features.
   Removes taxlots with extreme skinny shapes (4*area/perimeter^2 < 0.02) or very large area (>10,000,000 sqm)
 - Uses `ST_MakeValid()` to fix invalid geometries
 - Extracts only polygon geometries (`ST_CollectionExtract(..., 3)`)
 - Converts to MultiPolygon format for consistency

 Deduplication

- Identifies parcels with the same county and maptaxlot code that appear as multiple geometries
- Uses `ST_Union()` to merge these fragmented geometries into single MultiPolygons
- Addresses cases where the same maptaxlot code appears in multiple counties. Uses spatial 
  clustering (`ST_ClusterDBSCAN`) to group nearby geometries (within 500 meters).

 Final insert

- Calculates standardized area measurements in square meters using EPSG:5070 projection
- Generates geohash11 values for spatial indexing using EPSG:4326
- For remaining duplicates, appends a geohash suffix to the maptaxlot code.
*/
BEGIN;
DROP TABLE IF EXISTS s_washington_taxlots_post;
CREATE TABLE s_washington_taxlots_post (
    id SERIAL PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64) UNIQUE,
    landuse_cd SMALLINT,
    area_sqm DOUBLE PRECISION,
    geohash11 VARCHAR(11),
    geom GEOMETRY(MULTIPOLYGON, 3857)
);

DROP TABLE IF EXISTS consolidated_taxlots;
CREATE TABLE  consolidated_taxlots AS
WITH 
area_per AS (
    SELECT 
        id,
        ST_Perimeter(ST_Transform(geom, 5070)) AS perimeter,
        ST_Area(ST_Transform(geom, 5070)) AS area	
    FROM s_washington_taxlots
),
excluded_ids AS (
    SELECT t.id
    FROM s_washington_taxlots t
    JOIN area_per p ON t.id = p.id 
    WHERE 
        (
            NULLIF(TRIM(orig_parcel_id), '') IS NULL OR
            TRIM(orig_parcel_id) = '0' OR
            (
                NOT pg_input_is_valid(orig_parcel_id, 'numeric') 
                AND 
                orig_parcel_id !~* '(ALLOTTED|ONF|TRIBAL|DNR|BLM|STATE|WNF|ONP|IS|RES)'
                AND 
                landuse_cd IS NULL
            )
        )
        AND (
            4*area / POWER(perimeter, 2) < 0.02 
            OR
            area > 10000000
        )
)
SELECT 
    t.county_nm as county, 
    COALESCE(NULLIF(TRIM(orig_parcel_id), ''), LPAD(CAST(t.fips_nr AS VARCHAR), '000') || '_' || ST_GeoHash(ST_Transform(t.geom, 4326), 11)) as maptaxlot, 
    t.landuse_cd,
    ST_Area(ST_Transform(geom, 5070)) AS area_sqm, 
    ST_Perimeter(ST_Transform(geom, 5070)) AS perimeter,
    ST_GeoHash(ST_Transform(t.geom, 4326), 11) AS geohash11, 
    t.geom
FROM s_washington_taxlots t
WHERE NOT EXISTS (SELECT 1 FROM excluded_ids e WHERE e.id = t.id);

-- Fix geoms 
UPDATE consolidated_taxlots
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);

CREATE INDEX consolidated_taxlots_geom_idx
    ON consolidated_taxlots
    USING GIST (geom);

-- Assign a unique taxlot code to parcels with generic or missing maptaxlot usig geohash suffix
UPDATE consolidated_taxlots
SET maptaxlot = maptaxlot || '_' || UPPER(ST_GeoHash(ST_Transform(ST_Centroid(geom), 4326), 11))
WHERE maptaxlot ~* '(ALLOTTED|ONF|TRIBAL|DNR|BLM|STATE|WNF|ONP|IS|RES)' OR TRIM(maptaxlot) = '0';

-- Get unique taxlots
DROP TABLE IF EXISTS unique_taxlots;
CREATE TABLE unique_taxlots AS
SELECT
    t.county, 
    t.maptaxlot, 
    t.landuse_cd,
    area_sqm,
    perimeter,
    geohash11,
    geom AS geom
FROM consolidated_taxlots t
JOIN (
    SELECT maptaxlot
    FROM consolidated_taxlots
    GROUP BY maptaxlot
    HAVING COUNT(*) = 1
) u ON t.maptaxlot = u.maptaxlot;

-- Drop duplicated geometries from unique taxlots
-- 6514 records as of 2025-12
WITH dups AS (
    SELECT DISTINCT maptaxlot
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY geohash11 
                ORDER BY maptaxlot
            ) AS row_num
        FROM unique_taxlots
    ) 
    WHERE row_num > 1
)
DELETE FROM unique_taxlots 
WHERE maptaxlot IN (SELECT * FROM dups);

-- Remove skinny taxlots (roads, rivers)
DELETE FROM unique_taxlots
WHERE 4 * area_sqm / POWER(perimeter, 2) < 0.03;

-- Get duplicated taxlots
DROP TABLE IF EXISTS duplicated_taxlots;
CREATE TABLE duplicated_taxlots AS
SELECT
    t.county, 
    t.maptaxlot, 
    t.landuse_cd,
    area_sqm,
    ST_ClusterDBSCAN(ST_Transform(geom, 5070), eps := 500, minpoints := 1) OVER (PARTITION BY t.maptaxlot) AS cid,
    geom AS geom
FROM consolidated_taxlots t
JOIN (
    SELECT DISTINCT maptaxlot
    FROM (
        SELECT * 
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY maptaxlot 
                    ORDER BY maptaxlot, county
                ) AS row_num
            FROM consolidated_taxlots
        ) d
        WHERE row_num > 1
    )
) d ON t.maptaxlot = d.maptaxlot;

CREATE INDEX duplicated_taxlots_geom_idx
    ON duplicated_taxlots
    USING GIST (geom);

-- Merge duplicated taxlots with spatial clustering
-- first merge clusters by county, maptaxlot, and cluster id
DROP TABLE IF EXISTS merged_clusters;
CREATE TABLE merged_clusters AS
SELECT
    county,
    maptaxlot,
    landuse_cd,
    cid, 
    ST_Area(ST_Union(ST_Transform(geom, 5070))) AS area_sqm,
    ST_Perimeter(ST_Union(ST_Transform(geom, 5070))) AS perimeter,
    ST_Union(geom) AS geom
FROM duplicated_taxlots
GROUP BY county, landuse_cd, maptaxlot, cid;

-- Now merge by maptaxlot and cluster id to catch taxlots 
-- that span counties
DROP TABLE IF EXISTS merged_clusters_round2;
CREATE TABLE merged_clusters_round2 AS
SELECT 
    b.county, 
    a.maptaxlot, 
    ST_Area(ST_Transform(a.geom, 5070)) AS area_sqm, 
    ST_GeoHash(ST_Transform(a.geom, 4326), 11) AS geohash11, 
    a.geom 
FROM (
    SELECT 
        maptaxlot, 
        cid, 
        ST_Union(geom) AS geom 
    FROM merged_clusters
    GROUP BY maptaxlot, cid
) a
JOIN (
    SELECT DISTINCT ON (maptaxlot, cid) 
        county, 
        maptaxlot, 
        cid
    FROM merged_clusters 
    ORDER BY maptaxlot, cid, area_sqm DESC
) b ON a.maptaxlot = b.maptaxlot AND a.cid = b.cid;


-- Insert deduplicated taxlots into final table (~6k records as of 2025-12)
INSERT INTO s_washington_taxlots_post (county, maptaxlot, landuse_cd, area_sqm, geohash11, geom)
SELECT * FROM (
    SELECT 
        county, maptaxlot, landuse_cd, area_sqm, 
        ST_GeoHash(ST_Transform(geom, 4326), 11) AS geohash11, geom
    FROM unique_taxlots
    
    UNION ALL

    SELECT 
        county, 
        -- Rename remaining duplicates by appending geohash suffix.
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY maptaxlot) > 1 
            THEN maptaxlot || '_' || UPPER(ST_GeoHash(ST_Transform(ST_Centroid(geom), 4326), 6))
            ELSE maptaxlot 
        END AS maptaxlot,
        landuse_cd,
        area_sqm,
        geohash11, 
        geom
    FROM merged_clusters_round2
    -- WHERE maptaxlot NOT IN (SELECT maptaxlot FROM merged_taxlots_dups_counties)

) AS final_set
ON CONFLICT (maptaxlot) DO NOTHING;


-- Report results
DROP TABLE IF EXISTS report_washington_taxlots;
CREATE TABLE report_washington_taxlots AS
SELECT 
    'Total unprocessed taxlots' AS report,
    (SELECT COUNT(*) FROM s_washington_taxlots) AS count
UNION ALL
SELECT 
    'Cleaned taxlots' AS report,
    (SELECT COUNT(*) FROM consolidated_taxlots) AS count
UNION ALL
SELECT 
    'Unique taxlots' AS report,
    (SELECT COUNT(*) FROM unique_taxlots) AS count
UNION ALL
SELECT 
    'Duplicated taxlots' AS report,
    (SELECT COUNT(DISTINCT maptaxlot) FROM duplicated_taxlots) AS count
UNION ALL
SELECT 
    'Deduplicated by clustering multipart taxlots' AS report,
    (SELECT COUNT(*) FROM merged_clusters_round2) AS count
UNION ALL
SELECT 
    'Deduplicated with geohash suffix' AS report,
    (SELECT COUNT(*) FROM s_washington_taxlots_post WHERE maptaxlot LIKE '%\_%') AS count
UNION ALL
SELECT 
    'Final taxlot count' AS report,
    (SELECT COUNT(*) FROM s_washington_taxlots_post) AS count
UNION ALL
SELECT 
    'Remaining duplicated geohash11 values' AS report,
    (SELECT COUNT(*) 
     FROM (
         SELECT
             geohash11,
             ROW_NUMBER() OVER (PARTITION BY geohash11 ORDER BY geohash11) AS row_num
         FROM s_washington_taxlots_post
     ) sub
     WHERE row_num > 1) AS count
UNION ALL
SELECT 
    'Remaining duplicated maptaxlot values' AS report,
    (SELECT COUNT(*) 
     FROM (
         SELECT
             maptaxlot,
             ROW_NUMBER() OVER (PARTITION BY maptaxlot ORDER BY maptaxlot) AS row_num
         FROM s_washington_taxlots_post
     ) sub
     WHERE row_num > 1) AS count;


-- Clean up after yourself
DROP TABLE IF EXISTS consolidated_taxlots;
DROP TABLE IF EXISTS unique_taxlots;
DROP TABLE IF EXISTS duplicated_taxlots;
DROP TABLE IF EXISTS merged_clusters;
DROP TABLE IF EXISTS merged_clusters_round2;
DROP TABLE IF EXISTS merged_taxlots_dups_counties;

SELECT * FROM report_washington_taxlots;

COMMIT;
