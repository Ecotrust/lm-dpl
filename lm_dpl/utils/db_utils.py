"""
Database utility functions.

Provides reusable database operations for inserting both spatial and non-spatial
data into PostgreSQL/PostGIS tables with multi-threading support.
"""

import math
import json
import sys
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Literal
from .logging_utils import get_project_logger

try:
    from osgeo import ogr
except ImportError:
    pass  # GDAL is optional; handle import errors where needed


# Initialize module logger
logger = get_project_logger(__name__)


def worker_insert(
    db_credentials,
    data_chunk,
    table_name,
    srid,
    columns,
    property_keys,
    has_geometry=True,
    geom_type: Literal["wkt", "geojson"] = "wkt",
):
    """
    Worker function to insert a chunk of data into a PostgreSQL/PostGIS table.
    This function is executed by each thread and manages its own database connection.

    Args:
        db_credentials: Database connection details
        data_chunk: List of data rows to insert
        table_name: Name of the target table
        srid: Spatial Reference System Identifier (required only for spatial data)
        columns: List of column names for the INSERT statement
        property_keys: List of keys to access values from the data
        has_geometry: Boolean indicating whether the data contains geometry (default: True)
        geom_type: Type of geometry format - 'wkt' or 'geojson' (default: 'wkt')
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_credentials)

        # Build INSERT statement based on whether geometry is present
        if has_geometry:
            # For spatial data: assume geometry is the last column
            if geom_type == "wkt":
                geom_function = f"ST_SetSRID(ST_GeomFromText(%s), {srid})"
            elif geom_type == "geojson":
                geom_function = f"ST_SetSRID(ST_GeomFromGeoJSON(%s), {srid})"
            else:
                raise ValueError(f"Unsupported geometry type: {geom_type}")

            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES ({", ".join(['%s'] * len(property_keys))}, {geom_function})
                ON CONFLICT DO NOTHING;
            """
        else:
            # For non-spatial data: all columns are regular values
            insert_sql = f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES ({", ".join(['%s'] * len(columns))})
                ON CONFLICT DO NOTHING;
            """

        with conn.cursor() as cursor:
            for feature in data_chunk:
                if has_geometry:
                    # Validate geometry for spatial data
                    try:
                        if geom_type == "wkt":
                            from shapely import wkt

                            wkt.loads(feature[-1])
                        elif geom_type == "geojson":
                            # Validate GeoJSON structure
                            geom_data = json.loads(feature[-1])
                            if (
                                not isinstance(geom_data, dict)
                                or "type" not in geom_data
                            ):
                                raise ValueError("Invalid GeoJSON structure")
                    except Exception as e:
                        logger.warning(
                            f"Invalid {geom_type.upper()} geometry. Skipping feature. Error: {e}"
                        )
                        continue
                cursor.execute(insert_sql, tuple(feature))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Database error in worker for table '{table_name}': {e}")
        logger.debug(f"Failed to insert chunk of {len(data_chunk)} records")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error in worker for table '{table_name}': {e}")
        logger.debug(f"Data chunk size: {len(data_chunk)}, Columns: {columns}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def import_layer(
    db_credentials: Dict[str, Any],
    data: List,
    table_name: str,
    columns: List[str],
    property_keys: List[str],
    srid: Optional[int] = None,
    has_geometry: bool = True,
    num_threads: int = 4,
    geom_type: Literal["wkt", "geojson"] = "wkt",
) -> None:
    """
    Inserts data into a PostgreSQL/PostGIS table using multiple threads.
    Supports both spatial and non-spatial data.

    Args:
        db_credentials: Database connection details
        data: Data to load
        table_name: Name of the target table
        columns: A list of column names for the INSERT statement
        property_keys: A list of keys to access values from the data
        srid: The Spatial Reference System Identifier for geometry data (required for spatial data)
        has_geometry: Boolean indicating whether the data contains geometry (default: True)
        num_threads: Number of threads to use for the insertion
        geom_type: Type of geometry format - 'wkt' or 'geojson' (default: 'wkt')
    """
    # Check if running in a test environment (e.g., pytest)
    if "pytest" in sys.modules:
        logger.warning("Pytest detected. Forcing single-threaded execution.")
        num_threads = 1

    # Validate parameters for spatial data
    if has_geometry and srid is None:
        raise ValueError("SRID must be provided for spatial data")
    if not has_geometry and srid is not None:
        logger.warning(
            "SRID provided for non-spatial data - this parameter will be ignored"
        )
    if has_geometry and geom_type not in ["wkt", "geojson"]:
        raise ValueError("geom_type must be either 'wkt' or 'geojson'")

    try:
        # Handle empty data case
        if not data:
            logger.info(f"No data to import into {table_name}")
            return

        # Split the data into chunks for each thread
        chunk_size = math.ceil(len(data) / num_threads)
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Each thread will manage its own connection
            futures = [
                executor.submit(
                    worker_insert,
                    db_credentials,
                    chunk,
                    table_name,
                    srid,
                    columns,
                    property_keys,
                    has_geometry,
                    geom_type,
                )
                for chunk in chunks
            ]

            # Wait for all threads to complete
            for future in futures:
                future.result()  # Wait for the thread to finish and raise exceptions if any

        data_type = "spatial" if has_geometry else "non-spatial"
        logger.info(
            f"Successfully imported {len(data)} {data_type} records into {table_name}"
        )

    except psycopg2.Error as e:
        logger.error(f"Database import failed for table '{table_name}': {e}")
        logger.debug(
            f"Import parameters - Data size: {len(data)}, Columns: {columns}, Has geometry: {has_geometry}, Geometry type: {geom_type}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error during import to table '{table_name}': {e}")
        logger.debug(
            f"Import parameters - Data size: {len(data)}, Columns: {columns}, Has geometry: {has_geometry}, Geometry type: {geom_type}"
        )
        raise


def import_geospatial_layer(
    db_credentials: Dict[str, Any],
    data: List,
    table_name: str,
    srid: int,
    columns: List[str],
    property_keys: List[str],
    num_threads: int = 4,
    geom_type: Literal["wkt", "geojson"] = "wkt",
) -> None:
    """
    Inserts GeoJSON features into a PostGIS table using multiple threads.
    (Legacy function - maintained for backward compatibility)

    Args:
        db_credentials: Database connection details
        data: GeoJSON data to load
        table_name: Name of the target table
        srid: The Spatial Reference System Identifier for the geometry data
        columns: A list of column names for the INSERT statement
        property_keys: A list of keys to access values from the GeoJSON properties
        num_threads: Number of threads to use for the insertion
        geom_type: Type of geometry format - 'wkt' or 'geojson' (default: 'wkt')
    """
    import_layer(
        db_credentials=db_credentials,
        data=data,
        table_name=table_name,
        columns=columns,
        property_keys=property_keys,
        srid=srid,
        has_geometry=True,
        num_threads=num_threads,
        geom_type=geom_type,
    )


def create_table_from_layer(
    db_credentials: Dict[str, Any],
    layer,
    table_name: str,
    srid: Optional[int] = None,
) -> None:
    """
    Creates a PostGIS table based on the schema of an OGR layer.

    Args:
        db_credentials: Database connection details.
        layer: OGR layer object.
        table_name: Name of the target table.
        srid: Spatial Reference System Identifier for the geometry column.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_credentials)
        cursor = conn.cursor()

        # Get layer definition
        layer_defn = layer.GetLayerDefn()

        # Build column definitions
        column_defs = []
        for i in range(layer_defn.GetFieldCount()):
            field_defn = layer_defn.GetFieldDefn(i)
            field_name = field_defn.GetName()
            field_type = field_defn.GetType()

            # Map OGR field types to PostgreSQL types
            if field_type == ogr.OFTInteger:
                pg_type = "INTEGER"
            elif field_type == ogr.OFTInteger64:
                pg_type = "BIGINT"
            elif field_type == ogr.OFTReal:
                pg_type = "DOUBLE PRECISION"
            elif field_type == ogr.OFTString:
                pg_type = "TEXT"
            elif field_type == ogr.OFTDate:
                pg_type = "DATE"
            elif field_type == ogr.OFTDateTime:
                pg_type = "TIMESTAMP"
            else:
                pg_type = "TEXT"  # Default fallback

            column_defs.append(f"{field_name} {pg_type}")

        # Add geometry column
        geom_type_str = "GEOMETRY"  # Generic geometry type

        if srid:
            column_defs.append(f"geom geometry({geom_type_str}, {srid})")
        else:
            column_defs.append(f"geom geometry({geom_type_str})")

        # Create table
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (ID BIGSERIAL PRIMARY KEY, {", ".join(column_defs)})'
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Created table '{table_name}' with {len(column_defs)} columns")

    except psycopg2.Error as e:
        logger.error(f"Database error creating table '{table_name}': {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating table '{table_name}': {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def import_from_file(
    db_credentials: Dict[str, Any],
    file_path: str,
    table_name: str,
    num_threads: int = 4,
) -> None:
    """
    Imports data from a vector file (e.g., SHP or GeoJSON) into a PostGIS table.

    Args:
        db_credentials: Database connection details.
        file_path: Path to the vector file.
        table_name: Name of the target table.
        num_threads: Number of threads to use for the insertion.
    """
    if "osgeo.ogr" not in sys.modules:
        logger.error("The 'osgeo.ogr' library is required but not available.")
        raise ImportError(
            "The 'osgeo.ogr' library is required to create tables from layers."
        )

    data_source = ogr.Open(file_path, 0)
    if data_source is None:
        logger.error(f"Could not open file: {file_path}")
        raise IOError(f"Could not open file: {file_path}")

    layer = data_source.GetLayer()
    if layer is None:
        logger.error(f"No layer found in file: {file_path}")
        raise ValueError(f"No layer found in file: {file_path}")

    # Get SRID
    srs = layer.GetSpatialRef()
    if srs:
        srs.AutoIdentifyEPSG()
        srid = srs.GetAuthorityCode(None)
        if srid is None:
            logger.warning(
                f"Could not determine SRID for {file_path}. Defaulting to None."
            )
    else:
        srid = None
        logger.warning(
            f"No spatial reference found for {file_path}. Defaulting to None."
        )

    # Create table before inserting data
    create_table_from_layer(db_credentials, layer, table_name, srid)

    # Get columns and property keys
    layer_defn = layer.GetLayerDefn()
    columns = [
        layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())
    ]
    property_keys = list(columns)
    columns.append("geom")

    # Prepare data for insertion
    logger.info(f"Preparing data from layer with {layer.GetFeatureCount()} features")
    data_to_insert = []
    for feature in layer:
        properties = [feature.GetField(key) for key in property_keys]
        geom = feature.GetGeometryRef()
        if geom:
            properties.append(geom.ExportToWkt())
        data_to_insert.append(properties)

    # Import into PostGIS
    logger.info(f"Importing features into '{table_name}'")
    import_layer(
        db_credentials=db_credentials,
        data=data_to_insert,
        table_name=table_name,
        columns=columns,
        property_keys=property_keys,
        srid=srid,
        has_geometry=True,
        num_threads=num_threads,
        geom_type="wkt",
    )
