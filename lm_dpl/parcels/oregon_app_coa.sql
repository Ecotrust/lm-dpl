-- COA table
DROP TABLE IF EXISTS oregon_app_coa;
CREATE TABLE IF NOT EXISTS oregon_app_coa (
    id SERIAL PRIMARY KEY,
    huc12 VARCHAR(24) NOT NULL,
    coa_name VARCHAR(255) DEFAULT NULL,
    coa_id VARCHAR(6) DEFAULT NULL,
    ecoregion VARCHAR(255) DEFAULT NULL,
    profile_link VARCHAR(255) DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

WITH coa_join AS (
    SELECT
        h.huc12,
        c.coaid,
        c.coaname,
        c.centroidec,
        c.profilelin,
        h.geom as geometry
    FROM s_oregon_coa c
    JOIN s_oregon_huc h ON ST_Intersects(c.geom, h.geom)
)
INSERT INTO oregon_app_coa (
    huc12, 
    coa_id, 
    coa_name, 
    ecoregion, 
    profile_link, 
    geometry
)
SELECT
    huc12,
    coaid,
    coaname,
    centroidec,
    profilelin,
    geometry
FROM coa_join;
