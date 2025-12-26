BEGIN;
CREATE INDEX IF NOT EXISTS s_oregon_plss1_geom_idx
    ON s_oregon_plss1
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS s_oregon_plss2_geom_idx
  ON s_oregon_plss2
  USING GIST (geom);

-- Create/re-create table to hold the intersected PLSS tables
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

-- Fix invalid geometries
UPDATE s_oregon_plss
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);
COMMIT;