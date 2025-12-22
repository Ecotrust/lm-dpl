/* 
 This script performs the following steps:

 Cleaning and filtering

 - Removes records with NULL or empty maptaxlot values
 - Filters out taxlots with codes ending in specific patterns indicating non-parcel features:
   ROAD, WATER, RAIL, NONTL, CANAL, RIVER, GAP, RR, WTR, STR, ISLAND, R/R
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
- Generates geohash10 values for spatial indexing using EPSG:4326
- For remaining duplicates, appends a geohash suffix to the maptaxlot code.
*/
BEGIN;

DROP TABLE IF EXISTS s_oregon_taxlots_post;
CREATE TABLE s_oregon_taxlots_post (
    id SERIAL PRIMARY KEY,
    county VARCHAR(50),
    maptaxlot VARCHAR(64) UNIQUE,
    area_sqm DOUBLE PRECISION,
    geohash11 VARCHAR(11),
    geom GEOMETRY(MULTIPOLYGON, 3857)
);

DROP TABLE IF EXISTS consolidated_taxlots;
CREATE TABLE  consolidated_taxlots AS
SELECT county_name as county, TRIM(maptaxlot) AS maptaxlot, ST_Area(ST_Transform(geom, 5070)) AS area_sqm, geom 
FROM s_oregon_taxlots t
JOIN s_oregon_county_fips_mapping m
    ON t.county = m.county_name 
WHERE NULLIF(TRIM(maptaxlot), '') IS NOT NULL
    AND maptaxlot !~* '(ROAD|WATER|RAIL|NONTL|CANAL|RIVER|GAP|RR|WTR|STR|ISLAND|R\/R)';

-- Fix geoms 
UPDATE consolidated_taxlots
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);

CREATE INDEX consolidated_taxlots_geom_idx
    ON consolidated_taxlots
    USING GIST (geom);

-- Get unique taxlots
DROP TABLE IF EXISTS unique_taxlots;
CREATE TABLE unique_taxlots AS
SELECT
    t.county, 
    t.maptaxlot, 
    area_sqm,
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
                PARTITION BY ST_GeoHash(ST_Transform(geom, 4326), 11)  
                ORDER BY maptaxlot
            ) AS row_num
        FROM unique_taxlots
    ) 
    WHERE row_num > 1
)
DELETE FROM unique_taxlots 
WHERE maptaxlot IN (SELECT * FROM dups);

-- Get duplicated taxlots
DROP TABLE IF EXISTS duplicated_taxlots;
CREATE TABLE duplicated_taxlots AS
SELECT
    t.county, 
    t.maptaxlot, 
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
                    PARTITION BY county, maptaxlot 
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
    cid, 
    ST_Area(ST_Union(ST_Transform(geom, 5070))) AS area_sqm,
    ST_Union(geom) AS geom
FROM duplicated_taxlots
GROUP BY county, maptaxlot, cid;

-- Now merge by maptaxlot and cluster id to catch taxlots 
-- that span counties
DROP TABLE IF EXISTS merged_clusters_round2;
CREATE TABLE merged_clusters_round2 AS
SELECT 
    b.county, 
    a.maptaxlot, 
    ST_Area(ST_Transform(a.geom, 5070)) AS area_sqm, 
    ST_GeoHash(ST_Transform(a.geom, 4326), 11) AS geohash10, 
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
    SELECT DISTINCT ON (maptaxlot) 
        county, 
        maptaxlot, 
        cid
    FROM merged_clusters 
    ORDER BY maptaxlot, area_sqm DESC
) b ON a.maptaxlot = b.maptaxlot;

-- Get duplicated taxlots that were not merged
DROP TABLE IF EXISTS merged_taxlots_dups_counties;
CREATE TABLE merged_taxlots_dups_counties AS
WITH duplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY maptaxlot
            ORDER BY maptaxlot, area_sqm DESC
        ) AS row_num
    FROM (
        SELECT a.*
        FROM merged_clusters a
        JOIN merged_clusters b
            ON a.maptaxlot = b.maptaxlot 
            AND a.county <> b.county
    )
)
SELECT 
    county, 
    maptaxlot, 
    area_sqm, 
    geom 
FROM duplicated
WHERE row_num = 1;

-- Insert deduplicated taxlots into final table (~6k records as of 2025-12)
INSERT INTO s_oregon_taxlots_post (county, maptaxlot, area_sqm, geohash10, geom)
SELECT * FROM (
    SELECT 
        county, maptaxlot, area_sqm, 
        ST_GeoHash(ST_Transform(geom, 4326), 11) AS geohash10, geom
    FROM unique_taxlots
    
    UNION ALL
    
    SELECT 
        county, maptaxlot, 
        area_sqm,
        ST_GeoHash(ST_Transform(geom, 4326), 11) AS geohash10, geom
    FROM merged_taxlots_dups_counties

    UNION ALL

    SELECT 
        county, 
        -- Rename remaining duplicates by appending geohash suffix (~111 records as of 2025-12)
        CASE 
            WHEN COUNT(*) OVER (PARTITION BY maptaxlot) > 1 
            THEN maptaxlot || '_' || UPPER(ST_GeoHash(ST_Transform(ST_Centroid(geom), 4326), 6))
            ELSE maptaxlot 
        END AS maptaxlot,
        area_sqm,
        geohash10, 
        geom
    FROM merged_clusters_round2
    WHERE maptaxlot NOT IN (SELECT maptaxlot FROM merged_taxlots_dups_counties)

) AS final_set
ON CONFLICT (maptaxlot) DO NOTHING;


-- Report results
SELECT 
    'Total unprocessed taxlots' AS metric_name,
    (SELECT COUNT(*) FROM s_oregon_taxlots) AS metric_count
UNION ALL
SELECT 
    'Cleaned taxlots' AS metric_name,
    (SELECT COUNT(*) FROM consolidated_taxlots) AS metric_count
UNION ALL
SELECT 
    'Unique taxlots',
    (SELECT COUNT(*) FROM unique_taxlots)
UNION ALL
SELECT 
    'Duplicated taxlots',
    (SELECT COUNT(DISTINCT maptaxlot) FROM duplicated_taxlots)
UNION ALL
SELECT 
    'Deduplicated by clustering multipart taxlots',
    (SELECT COUNT(*) FROM merged_clusters_round2)
UNION ALL
SELECT 
    'Deduplicated with geohash suffix',
    (SELECT COUNT(*) FROM s_oregon_taxlots_post WHERE maptaxlot LIKE '%\_%')
UNION ALL
SELECT 
    'Final taxlot count',
    (SELECT COUNT(*) FROM s_oregon_taxlots_post)
UNION ALL
SELECT 
    'Remaining duplicated geohash11 values',
    (SELECT COUNT(*) 
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY geohash10 
                ORDER BY geohash10             
            ) AS row_num
        FROM s_oregon_taxlots_post
    ) 
    WHERE row_num > 1)
UNION ALL
SELECT 
    'Remaining duplicated maptaxlot values',
    (SELECT COUNT(*) 
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY maptaxlot 
                    ORDER BY maptaxlot 
                ) AS row_num
            FROM s_oregon_taxlots_post
        ) 
    WHERE row_num > 1
    );


-- Clean up after yourself
DROP TABLE IF EXISTS consolidated_taxlots;
DROP TABLE IF EXISTS unique_taxlots;
DROP TABLE IF EXISTS duplicated_taxlots;
DROP TABLE IF EXISTS merged_clusters;
DROP TABLE IF EXISTS merged_clusters_round2;

COMMIT;
-- END;

/* Test why these taxlots are missing after deduplication
select * from merged_taxlots
where maptaxlot in ('07190000-02400', '082436DA-02300', '082436AC-00300', '082436AA-02800', '08250000-01300', '08190000-00900')
order by maptaxlot
*/
