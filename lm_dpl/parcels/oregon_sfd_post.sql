-- Fix invalid geometries
UPDATE s_oregon_sfd
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);
