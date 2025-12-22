"""Module for calculating parcel elevation statistics from DEM data.

This module processes taxlot geometries against Digital Elevation Model (DEM) data
to extract minimum and maximum elevation values for each parcel. It also provides
utilities for downloading GEE data using PyTorch DataLoader and TileGeoSampler.
"""

import os
import logging
import multiprocessing
from typing import Optional, Tuple

import ee
import numpy as np
import pandas as pd
from tqdm import tqdm

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import geopandas as gpd
from torchgeo.datasets import BoundingBox

from forestvision.processing import ZonalStatsBase
from forestvision.datasets import GPDFeatureCollection
from forestvision.deploy import AnyRasterDataset

from lm_dpl.utils import get_config, get_project_logger
from functools import partial

# Configuration constants
CHUNKSIZE = 50000
MAX_WORKERS = 40
MIN_AREA_THRESHOLD = 2023  # 1/2 acre in square meters

config = get_config()
logger = get_project_logger(__name__)

ee.Initialize(project=config.GEE_PROJECT)
# Mute warnings from rasterio.merge
logging.getLogger("rasterio.merge").setLevel(logging.WARNING)


class SumParcel(ZonalStatsBase):
    """Zonal statistics processor for calculating parcel elevation statistics.

    This class extends ZonalStatsBase to compute minimum and maximum elevation
    values from DEM data for individual taxlot parcels.
    """

    def reduce_func(
        self,
        bbox: BoundingBox,
        row: object,
        data: np.ndarray,
        zones: np.ndarray,
    ) -> Optional[Tuple[int, str, int, int, int, int, float]]:
        """Calculate minimum and maximum elevation values for a parcel.

        Args:
            bbox: Bounding box of the current feature.
            row: Feature row containing parcel metadata.
            data: Elevation data array from DEM.
            zones: Zone data array (unused in this implementation).

        Returns:
            Tuple containing (parcel_id, geohash11, min_elevation, max_elevation, forest_pix, total_pix, area_sqm).

        Note:
            Elevation values are converted to integers. NaN values are ignored
            in the calculation.
        """
        # Convert data to float32 and handle nodata values
        data = data.astype(np.float32)
        data[data == self.nodata] = np.nan
        zones = zones.astype(np.float32)
        zones[zones == self.nodata] = np.nan

        # Calculate min and max elevation, ignoring NaN values
        min_elev = np.nanmin(data).item()
        max_elev = np.nanmax(data).item()

        # Convert to integers (handle all values including 0)
        min_elev = int(min_elev) if not np.isnan(min_elev) else 0
        max_elev = int(max_elev) if not np.isnan(max_elev) else 0
        # forest_pix = np.count_nonzero(zones[~np.isnan(zones)]).item()
        unique_values, counts = np.unique(zones[~np.isnan(zones)], return_counts=True)
        hist = dict(zip(unique_values.astype(int), counts.astype(int)))
        forest_pix = hist.get(1, 0)  # Assuming '1' represents
        total_pix = sum(counts)

        return (
            row.maptaxlot,
            row.geohash11,
            min_elev,
            max_elev,
            forest_pix,
            total_pix,
            row.area_sqm.item(),
        )


def process_taxlot(zonal: SumParcel, index: int) -> Optional[Tuple[int, int, int]]:
    """Process a single taxlot to extract elevation statistics.

    Args:
        zonal: Zonal statistics processor instance.
        index: Index of the taxlot feature to process.

    Returns:
        Tuple of (parcel_id, min_elevation, max_elevation) or None if processing fails.
    """
    try:
        return zonal[index]
    except (IndexError, ValueError, AttributeError) as e:
        logger.warning(f"Error processing feature index {index}: {e}")
        return None


def main(state: str) -> None:
    """Main function to process taxlot elevation statistics.

    This function:
    1. Connects to the database and loads DEM data
    2. Reads taxlot geometries in chunks
    3. Processes each chunk to extract elevation statistics
    4. Saves results to the database

    Raises:
        ValueError: If DEM dataset or taxlot data is invalid.
        RuntimeError: If database operations fail.
    """
    # Create SQLAlchemy engine with context manager for proper cleanup
    engine = create_engine(config.postgres_url)

    try:
        # Validate and load DEM dataset
        start_time = pd.Timestamp.now()
        logger.info(f"Process started at {start_time}")
        if not hasattr(config, "DEM_PATH") or not config.DEM_PATH:
            raise ValueError("DEM_PATH configuration is missing or empty")

        q_create_table = f"""
            CREATE TABLE IF NOT EXISTS public.s_{state}_elevation
            (
                maptaxlot VARCHAR(25) PRIMARY KEY,
                geohash11 VARCHAR(11),
                min_elev INTEGER,
                max_elev INTEGER,
                forest_pix INTEGER,
                total_pix INTEGER,
                area_sqm FLOAT
            );

            DROP INDEX IF EXISTS s_{state}_elevation_hash;
        """

        # Select new taxlots or taxlots with changed geometry
        query = f"""
            SELECT maptaxlot, geohash11, geom as geometry, area_sqm
            FROM s_{state}_taxlots_post
            WHERE area_sqm > {MIN_AREA_THRESHOLD}
                -- AND objectid NOT IN (1618482, 1618481)
                -- AND county_nm = 'Lincoln'

            EXCEPT 
            
            SELECT t.maptaxlot, t.geohash11, t.geom as geometry, t.area_sqm
            FROM s_{state}_taxlots_post t
            JOIN s_{state}_elevation e 
                ON t.maptaxlot = e.maptaxlot 
                AND t.geohash11 = e.geohash11
            -- LIMIT 1000;
        """

        with engine.begin() as conn:
            conn.execute(text(q_create_table))
            conn.commit()

        chunks = gpd.read_postgis(
            query,
            engine,
            geom_col="geometry",
            chunksize=CHUNKSIZE,
        )

        logger.info(f"Starting parallel processing with {MAX_WORKERS} workers")

        chunk_count = 0
        total_processed = 0
        total_failed = 0

        for chunk in tqdm(chunks, desc="Processing chunks"):
            # Create transaction for each chunk with savepoint
            with engine.begin() as conn:
                savepoint = conn.begin_nested()

                # Validate chunk data
                if chunk.empty:
                    logger.warning("Empty chunk encountered, skipping")
                    continue

                taxlots = GPDFeatureCollection(chunk.to_crs(epsg="5070"))

            dem = AnyRasterDataset(
                paths=os.path.join(config.DATADIR, "gee3dep"),
                glob="*.tif",
                nodata=-9999,
                is_image=True,
                res=10,
            )
            dw = AnyRasterDataset(
                paths=os.path.join(config.DATADIR, "geedynamicworld"),
                glob="*.tif",
                nodata=-9999,
                is_image=False,
                res=10,
            )

            zonal = SumParcel(taxlots, dem, dw, data_key="image")
            _process_taxlot = partial(process_taxlot, zonal)

            with multiprocessing.Pool(processes=MAX_WORKERS) as pool:
                total_features = len(zonal)
                data_rows = list(
                    tqdm(
                        pool.map(_process_taxlot, range(total_features)),
                        desc=f"Processing features in chunk {chunk_count}",
                        total=total_features,
                    )
                )

                # Filter out None values from failed processing
                valid_data_rows = [row for row in data_rows if row is not None]

                if valid_data_rows:
                    df = pd.DataFrame(
                        valid_data_rows,
                        columns=[
                            "maptaxlot",
                            "geohash11",
                            "min_elev",
                            "max_elev",
                            "forest_pix",
                            "total_pix",
                            "area_sqm",
                        ],
                    )

                    # Convert to list of dicts for batch execution
                    data_rows_dicts = df.to_dict("records")

                    upsert_sql = f"""
                        INSERT INTO public.s_{state}_elevation 
                        (maptaxlot, geohash11, min_elev, max_elev, forest_pix, total_pix, area_sqm)
                        VALUES (:maptaxlot, :geohash11, :min_elev, :max_elev, :forest_pix, :total_pix, :area_sqm)
                        ON CONFLICT (maptaxlot) DO UPDATE SET
                            min_elev = EXCLUDED.min_elev,
                            max_elev = EXCLUDED.max_elev,
                            forest_pix = EXCLUDED.forest_pix,
                            total_pix = EXCLUDED.total_pix,
                            area_sqm = EXCLUDED.area_sqm;
                    """

                    try:
                        # Process in batches to manage memory and transaction size
                        batch_size = 1000
                        with engine.begin() as conn:
                            for i in range(0, len(data_rows_dicts), batch_size):
                                batch = data_rows_dicts[i : i + batch_size]
                                conn.execute(text(upsert_sql), batch)

                        total_processed += len(valid_data_rows)
                        logger.info(
                            f"Processed {len(valid_data_rows)} features in chunk {chunk_count}"
                        )

                    except SQLAlchemyError as e:
                        logger.error(f"Database error in chunk {chunk_count}: {e}")
                        total_failed += len(valid_data_rows)
                        continue
                else:
                    logger.warning(f"No valid data processed in chunk {chunk_count}")

                chunk_count += 1

        logger.info(
            f"Completed processing. Total features processed: {total_processed}, "
            f"failed: {total_failed}"
        )

    except Exception as e:
        logger.error(f"Fatal error in main processing: {e}")
        raise RuntimeError(f"Processing failed: {e}") from e
    finally:
        # Ensure engine is disposed
        engine.dispose()
        end_time = pd.Timestamp.now()
        logger.info(f"Process ended at {end_time}")


if __name__ == "__main__":
    main("oregon")
