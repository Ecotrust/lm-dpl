-- ForestType table
DROP TABLE IF EXISTS app_foresttype;
CREATE TABLE app_foresttype (
    id BIGSERIAL PRIMARY KEY,
    fortype VARCHAR(255) DEFAULT NULL,
    symbol VARCHAR(10) DEFAULT NULL,
    can_class VARCHAR(255) DEFAULT NULL,
    diameter VARCHAR(255) DEFAULT NULL,
    geometry geometry(MULTIPOLYGON, 3857) DEFAULT NULL
);

-- The End