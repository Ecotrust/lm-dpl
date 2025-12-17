BEGIN;
UPDATE s_washington_plss
SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3))
WHERE NOT ST_IsValid(geom);


-- Delete large polygons from county, state, and layer bounds
DELETE FROM s_washington_plss 
WHERE ST_Area(geom) >= 10000000;
COMMIT;
