"""
SSURGO Data Processor - A class for fetching and processing SSURGO soil data.
"""

import os
from typing import List, Optional, Dict, Any
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from ..clients import SDADataQ
from ..clients.db_manager import DatabaseManager
from ..utils import import_geospatial_layer, import_layer, get_project_logger


class ConcurrentFetcher:
    """A context manager for fetching data concurrently in batches using multithreading."""

    def __init__(self, func_to_exec, q, items, batch_size, max_workers):
        """Initialize concurrent fetcher.

        Args:
            func_to_exec: Function to execute for each batch.
            q: Query template with a placeholder for items.
            items: List of items to process in batches.
            batch_size: Number of items per batch.
            max_workers: Number of concurrent threads.

        Yields:
            A generator that yields batch results as they become available.
        """
        self.func_to_exec = func_to_exec
        self.q = q
        self.items = items
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.executor = None
        self.batches = []
        self.future_to_batch = {}

    def __enter__(self):
        self.batches = [
            self.items[i : i + self.batch_size]
            for i in range(0, len(self.items), self.batch_size)
        ]
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.future_to_batch = {
            self.executor.submit(self.func_to_exec, self.q, batch): batch
            for batch in self.batches
        }
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.executor:
            self.executor.shutdown(wait=True)

    def __iter__(self):
        for future in tqdm(
            as_completed(self.future_to_batch),
            total=len(self.batches),
            desc=f"Fetching data in {len(self.batches)} batches",
        ):
            try:
                yield future.result()
            except Exception as e:
                # logging.error(f"A batch failed to fetch: {e}")
                yield None


class SSURGOProcessor:
    """
    A class for processing SSURGO soil data from NRCS Soil Data Access services.

    This class integrates with the client architecture to provide a unified
    interface for fetching and processing SSURGO spatial and tabular data.
    """

    mukey_q = """
        SELECT DISTINCT areasymbol, mukey 
        FROM mupolygon
        WHERE areasymbol LIKE '{state}%'
    """

    sp_q = """
        SELECT DISTINCT
            mapunit.mukey, 
            mp.areasymbol,
            mp.musym,
            mp.spatialversion,
            G.MupolygonWktWm as geom
        FROM mapunit
        JOIN (
            SELECT DISTINCT mukey, spatialversion, musym, areasymbol
            FROM mupolygon
        ) as mp ON mapunit.mukey = mp.mukey
        CROSS APPLY SDA_Get_MupolygonWktWm_from_Mukey(mapunit.mukey) as G
        WHERE mp.mukey in ({mukeys_str})
    """

    tb_q = """
        WITH component_data AS (
            -- 1. Get component data and calculate percentage
            SELECT
                mukey,
                cokey,
                comppct_r,
                (comppct_r / 100.0) AS comppct_p
            FROM
                component
            WHERE
                mukey IN ({mukeys_str}) 
        ),
        coforprod_labeled AS (
            -- 2. Join component with forest productivity, create si_label, and rank for aggregation
            SELECT
                c.mukey,
                (cfp.plantcomname + ' - ' + CAST(cfp.siteindex_r AS VARCHAR(10)) + ' ft') AS si_label,
                ROW_NUMBER() OVER(PARTITION BY c.mukey ORDER BY c.comppct_r DESC, c.cokey) as rn
            FROM
                coforprod cfp
            JOIN
                component_data c ON cfp.cokey = c.cokey
            WHERE
                cfp.siteindex_r IS NOT NULL
        ),
        coforprod_agg AS (
            -- 3. Aggregate forest productivity, taking the first ranked label
            SELECT
                mukey,
                si_label
            FROM
                coforprod_labeled
            WHERE
                rn = 1
        ),
        restrictions_agg AS (
            -- 4. Calculate weighted average for restrictive layer depths
            SELECT
                c.mukey,
                SUM(cr.resdept_l * c.comppct_p) AS avg_rs_l,
                SUM(cr.resdept_h * c.comppct_p) AS avg_rs_h
            FROM
                corestrictions cr
            JOIN
                component_data c ON cr.cokey = c.cokey
            GROUP BY
                c.mukey
        )
        -- 5. Join processed data
        SELECT DISTINCT
            m.mukey,
            m.muname,
            COALESCE(m.drclassdcd, 'No Data Available') AS drclassdcd,
            m.forpehrtdcp,
            COALESCE(cfp.si_label, 'None') AS si_label,
            COALESCE(CAST(res.avg_rs_l AS DECIMAL(6,2)), 0) AS avg_rs_l,
            COALESCE(CAST(res.avg_rs_h AS DECIMAL(6,2)), 0) AS avg_rs_h
        FROM
            muaggatt m
        LEFT JOIN
            coforprod_agg cfp ON m.mukey = cfp.mukey
        LEFT JOIN
            restrictions_agg res ON m.mukey = res.mukey
        WHERE
            m.mukey IN ({mukeys_str}); 
    """

    def __init__(self, state: str):
        """Initialize the SSURGO processor with SDA clients."""
        self.logger = get_project_logger(__name__)
        self.sda_client = SDADataQ()
        self.logger.info(f"Initializing SSURGO processor for state: {state}")
        self.mukeys = self.fetch_mukeys(state)
        self.logger.info(f"Found {len(self.mukeys)} MUKEYS for state {state}")

    def _fetch_data(self, q: str, mukeys: List[str]) -> Optional[List[Dict[str, Any]]]:
        """
        Worker function to fetch data for a single batch of MUKEYS.

        Args:
            q: SQL query template with a placeholder for mukeys
            mukeys: List of mapunit keys to fetch data for

        Returns:
            List of dictionaries with query results, or None if failed.
        """
        if not mukeys:
            return None
        mukeys_str = ",".join(f"'{key}'" for key in mukeys)
        query = q.format(mukeys_str=mukeys_str)
        return self.sda_client.run_query(query)

    def fetch_mukeys(
        self,
        state: str,
    ) -> Optional[List[str]]:
        """
        Fetch MUKEYS for a given area symbol.

        Args:
            state: State two-letter code (e.g., 'OR' for Oregon)
        Returns:
            List of MUKEYS for the specified state
        """
        return [
            row[1]
            for row in self.sda_client.run_query(self.mukey_q.replace("{state}", state))
        ]

    def fetch_tb(
        self,
        db_credentials: Dict[str, Any],
        concurrent: bool = True,
        batch_size: int = None,
        max_workers: int = None,
    ) -> None:
        """
        Fetch tabular SSURGO data for a list of MUKEYS concurrently.

        Args:
            mukeys: List of mapunit keys. If None, uses all mukeys for the state.
            concurrent: Whether to fetch data concurrently using multiple threads.
            batch_size: Number of mukeys per batch.
            max_workers: Number of concurrent threads.

        Returns:
            A pandas DataFrame with the tabular data, or None if failed.
        """

        self.logger.info(f"Fetching tabular data for {len(self.mukeys)} mukeys...")

        # For most cases we won't need concurrency
        all_results = []
        if concurrent:
            if batch_size is None or max_workers is None:
                raise ValueError(
                    "batch_size and max_workers must be provided for concurrent fetching"
                )
            with ConcurrentFetcher(
                self._fetch_data, self.tb_q, self.mukeys, batch_size, max_workers
            ) as fetcher:
                # Iterate over batch results as they become available
                for batch_result in fetcher:
                    if batch_result:
                        import_layer(
                            db_credentials=db_credentials,
                            data=batch_result,
                            table_name="s_ssurgo_data",
                            columns=[
                                "mukey",
                                "muname",
                                "drclassdcd",
                                "forpehrtdcp",
                                "si_label",
                                "avg_rs_l",
                                "avg_rs_h",
                            ],
                            property_keys=[
                                "mukey",
                                "muname",
                                "drclassdcd",
                                "forpehrtdcp",
                                "si_label",
                                "avg_rs_l",
                                "avg_rs_h",
                            ],
                            has_geometry=False,
                            num_threads=4,
                        )

    def fetch_geoms(
        self,
        db_credentials: Dict[str, Any],
        batch_size: int = 5,
        max_workers: int = 4,
    ) -> None:
        """
        Import processed SSURGO data to a PostGIS database.

        Args:
            data: List of data rows to insert
            columns: List of column names corresponding to the data
            db_credentials: Database connection details
            table_name: Name of the target table
            srid: Spatial Reference System Identifier (default: 3857 for Web Mercator)
        """
        # Use the context manager to fetch data concurrently
        with ConcurrentFetcher(
            self._fetch_data, self.sp_q, self.mukeys, batch_size, max_workers
        ) as fetcher:
            # Iterate over batch results as they become available
            for batch_result in fetcher:
                if batch_result:
                    import_geospatial_layer(
                        db_credentials=db_credentials,
                        data=batch_result,
                        table_name="s_ssurgo_geom",
                        srid=3857,
                        columns=["mukey", "areasym", "musym", "spatialversion", "geom"],
                        property_keys=["mukey", "areasym", "musym", "spatialversion"],
                        num_threads=1,
                    )


def main(state: str, config_path: Optional[str] = None) -> None:
    """
    Main entry point for SSURGO processing from CLI.

    Args:
        state: Two-letter state code (e.g., 'OR', 'WA', 'CA')
        config_path: Optional path to configuration file
    """
    from ..utils.config import get_config
    from ..utils.logging_utils import setup_project_logging

    logger = setup_project_logging()

    # Validate state code
    if len(state) != 2:
        logger.error("State code must be exactly 2 characters (e.g., OR, WA, CA)")
        raise ValueError("State code must be exactly 2 characters")

    state = state.upper()

    logger.info(f"Starting SSURGO processing for state: {state}")

    config = get_config()
    dsn = config.postgres_dsn_dict

    # Initialize SSURGO processor with state
    ssurgo_processor = SSURGOProcessor(state=state)

    state_name = {
        "OR": "oregon",
        "WA": "washington",
    }

    try:
        with DatabaseManager(dsn) as db_manager:
            # Create staging schema
            sql_script_path = os.path.join(
                os.path.dirname(__file__), f"{state_name.get(state)}_soils_schema.sql"
            )
            db_manager.execute_from_file(sql_script_path)
            logger.info("Database staging schema created successfully")
        # Fetch and import ssurgo soil table
        ssurgo_processor.fetch_tb(db_credentials=dsn, batch_size=3000, max_workers=1)
        logger.info(f"SSURGO data processing completed successfully")
        # Fetch and import ssurgo geometries into PostGIS
        ssurgo_processor.fetch_geoms(db_credentials=dsn, batch_size=30, max_workers=15)
        logger.info("SSURGO geometry processing completed successfully")
    except Exception as e:
        logger.error(f"Error during SSURGO processing: {e}")
        raise


if __name__ == "__main__":
    import os

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="SSURGO Data Processor - Fetch and process SSURGO soil data"
    )
    parser.add_argument(
        "--state",
        "-s",
        type=str,
        required=True,
        help="Two-letter state code (e.g., OR, WA, CA, DE)",
    )

    args = parser.parse_args()

    # Call the main function
    main(args.state)
