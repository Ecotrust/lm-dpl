-- Fix invalid geometries
UPDATE s_oregon_sfd
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);
