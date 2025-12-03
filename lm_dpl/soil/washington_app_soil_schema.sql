DROP TABLE IF EXISTS s_washington_soil_data;
CREATE TABLE s_washington_soil_data
(
    id SERIAL PRIMARY KEY,
    mukey text COLLATE pg_catalog."default",
    muname text COLLATE pg_catalog."default",
    drclassdcd text COLLATE pg_catalog."default",
    forpehrtdcp text COLLATE pg_catalog."default",
    si_label text COLLATE pg_catalog."default",
    avg_rs_l double precision,
    avg_rs_h double precision
);

DROP TABLE IF EXISTS s_washington_soil_geom;
CREATE TABLE s_washington_soil_geom
(
    id SERIAL PRIMARY KEY,
    mukey VARCHAR(100) DEFAULT NULL,
    areasym VARCHAR(100) DEFAULT NULL,
    musym VARCHAR(100) DEFAULT NULL,
    spatialversion INTEGER DEFAULT NULL,
    geom geometry(MultiPolygon,3857)
);
