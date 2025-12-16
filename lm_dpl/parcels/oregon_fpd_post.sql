-- Fix invalid geometries
UPDATE s_washington_fpd
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);
