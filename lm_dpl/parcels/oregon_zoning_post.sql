UPDATE s_oregon_zoning
SET geom = ST_MakeValid(geom)
WHERE NOT ST_IsValid(geom);
