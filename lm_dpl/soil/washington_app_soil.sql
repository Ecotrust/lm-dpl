-- SoilType table
DROP TABLE IF EXISTS washington_app_soiltype;
CREATE TABLE washington_app_soiltype (
    id SERIAL PRIMARY KEY,
    mukey VARCHAR(100) DEFAULT NULL,
    areasym VARCHAR(100) DEFAULT NULL,
    spatial DOUBLE PRECISION DEFAULT NULL,
    musym VARCHAR(100) DEFAULT NULL,
    shp_lng DOUBLE PRECISION DEFAULT NULL,
    shap_ar DOUBLE PRECISION DEFAULT NULL,
    si_label VARCHAR(255) DEFAULT NULL,
    muname VARCHAR(255) DEFAULT NULL,
    drclssd VARCHAR(100) DEFAULT NULL,
    frphrtd VARCHAR(100) DEFAULT NULL,
    avg_rs_l DOUBLE PRECISION DEFAULT NULL,
    avg_rs_h DOUBLE PRECISION DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

WITH soils_join (
    mukey, 
    areasym, 
    spatial, 
    musym, 
    shp_lng, 
    shap_ar, 
    si_label, 
    muname, 
    drclssd, 
    frphrtd, 
    avg_rs_l, 
    avg_rs_h, 
    geometry
) AS (
    SELECT 
        s.mukey, 
        g.areasym, 
        g.spatialversion, 
        g.musym, 
        ST_Perimeter(g.geom), 
        ST_Area(g.geom),
        s.si_label, 
        s.muname, 
        s.drclassdcd, 
        s.forpehrtdcp, 
        s.avg_rs_l,
        s.avg_rs_h,
        g.geom
    FROM s_washington_soil_data AS s
    LEFT JOIN s_washington_soil_geom AS g
    ON s.mukey = g.mukey
)
INSERT INTO washington_app_soiltype (
    mukey, 
    areasym, 
    spatial, 
    musym, 
    shp_lng, 
    shap_ar, 
    si_label, 
    muname, 
    drclssd, 
    frphrtd, 
    avg_rs_l, 
    avg_rs_h, 
    geometry
)
SELECT
    mukey, 
    areasym, 
    spatial, 
    musym, 
    shp_lng, 
    shap_ar, 
    si_label, 
    muname, 
    drclssd, 
    frphrtd, 
    avg_rs_l, 
    avg_rs_h, 
    geometry
FROM soils_join;
