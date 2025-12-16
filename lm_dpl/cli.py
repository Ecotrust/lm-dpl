#!/usr/bin/env python3
"""
Unified CLI for Landmapper Data Pipeline Library (lm-dpl).
"""

import argparse
import sys
import logging
from typing import Optional
from lm_dpl.utils.db_utils import import_from_file
from lm_dpl.utils.config import get_config


def get_available_layers():
    """
    Get all available layer names from the REST client configuration.

    Returns:
        List of available layer names sorted alphabetically
    """
    from lm_dpl.clients.restclient import LandmapperRESTClient

    try:
        client = LandmapperRESTClient()
        all_layers = set()

        for state_name in client._config.keys():
            state_info = client.get_state_info(state_name)
            if state_info:
                all_layers.update(state_info.keys())

        # Always include soil and elevation as they're handled by separate processors
        all_layers.add("soil")
        all_layers.add("elevation")
        return sorted(list(all_layers))
    except Exception as e:
        logging.warning(f"Could not load layers from endpoints configuration: {e}")
        # Fallback to original hardcoded list, always include soil and elevation
        return sorted(
            [
                "elevation",
                "soil",
                "fpd",
                "zoning",
                "plss1",
                "plss2",
                "sfd",
                "taxlots",
                "coa",
            ]
        )


def normalize_state(state: str, to: str = "name") -> str:
    """
    Normalize state between full name and two-letter abbreviation.

    Args:
        state: State name or abbreviation
        to: 'name' to normalize to full name, 'abbr' to normalize to abbreviation

    Returns:
        Normalized state string
    """
    state_lower = state.lower()
    name_to_abbr = {
        "oregon": "or",
        "washington": "wa",
    }
    abbr_to_name = {v: k for k, v in name_to_abbr.items()}

    if to == "name":
        return abbr_to_name.get(state_lower, state_lower)
    elif to == "abbr":
        return name_to_abbr.get(state_lower, state_lower)
    else:
        raise ValueError("Parameter 'to' must be either 'name' or 'abbr'")


def run_fetch(
    state: str,
    layers: Optional[list[str]] = None,
    config_path: Optional[str] = None,
    overwrite: bool = False,
) -> int:
    """
    Fetch data from remote sources for the specified layers and state.

    Args:
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        layers: List of specific layers to fetch (optional, if None fetch all available)
        config_path: Optional path to custom endpoints configuration file
        overwrite: If True, drop and recreate tables before processing

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        from lm_dpl.parcels.processor import ParcelProcessor
        from lm_dpl.soil.processor import main as soil_main
        from lm_dpl.forest.parcel_elevation import main as elevation_main
        import yaml

        normalized_state = normalize_state(state)
        logging.info(f"Fetching data for {normalized_state}")

        # Handle soil and elevation data separately if they're in the layers list
        if layers:
            if "soil" in layers:
                normalized_state_abbr = normalize_state(state, to="abbr")
                soil_main(normalized_state_abbr, config_path)
                layers.remove("soil")

            if "elevation" in layers:
                elevation_main(normalized_state)
                layers.remove("elevation")

            if not layers:  # Only soil and/or elevation were requested
                return 0

        # Handle parcel-related layers
        processor = ParcelProcessor(normalized_state, config_path=config_path)

        if layers:
            for layer in layers:
                try:
                    processor.process_service(layer, overwrite=overwrite)
                except Exception as e:
                    logging.error(
                        f"Error fetching layer '{layer}' for state {normalized_state}: {e}"
                    )
                    return 1
        else:
            # Fetch all layers
            processor.fetch(overwrite=overwrite)

        return 0

    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return 1


def run_process(table: str, state: str) -> int:
    """
    Process data to generate application tables (app_taxlots, app_coa, app_soil, app_populationpoint).

    Args:
        table: Table to process (choices: taxlots, coa, soil, populationpoint)
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        from lm_dpl.parcels.processor import ParcelProcessor
        from lm_dpl.soil.processor import main as soil_main

        normalized_state = normalize_state(state)
        logging.info(f"Processing {table} table for {normalized_state}")

        if table == "taxlots":
            processor = ParcelProcessor(normalized_state)
            processor.process_app_taxlot()
        elif table == "coa":
            processor = ParcelProcessor(normalized_state)
            processor.process_app_coa()
        elif table == "soil":
            from lm_dpl.soil.processor import process_soil_table

            normalized_state = normalize_state(state)
            return process_soil_table(normalized_state)
        elif table == "populationpoint":
            processor = ParcelProcessor(normalized_state)
            processor.process_app_populationpoint()
        else:
            raise ValueError(f"Unknown table: {table}")

        return 0

    except Exception as e:
        logging.error(f"Error processing {table} for state {state}: {e}")
        return 1


def run_import_file(
    file_path: str,
    table_name: str,
    config_path: Optional[str] = None,
    srid: Optional[int] = None,
    t_srid: Optional[int] = None,
) -> int:
    """
    Import data from a file into the database.

    Args:
        file_path: Path to the file to import.
        table_name: Name of the target table.
        config_path: Optional path to configuration file.
        srid: Optional source SRID to override file auto-detection.
        t_srid: Optional target SRID for geometry reprojection.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        config = get_config()
        db_creds = config.postgres_dsn_dict
        import_from_file(db_creds, file_path, table_name, srid=srid, t_srid=t_srid)
        return 0
    except Exception as e:
        logging.error(f"Error importing file {file_path}: {e}")
        return 1


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Landmapper Data Pipeline Library (lm-dpl)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
lm-dpl fetch --layer fpd OR                  # Fetch FPD layer for Oregon
lm-dpl fetch --layer soil WA                 # Fetch soil data for Washington
lm-dpl fetch --layer fpd --layer plss1 OR    # Fetch multiple layers for Oregon
lm-dpl fetch --config config.yml OR          # Fetch all layers using custom config
lm-dpl fetch --overwrite --layer taxlots OR  # Fetch taxlots and drop existing data
lm-dpl process --table taxlots --state OR    # Process app_taxlots table for Oregon
lm-dpl process --table coa --state WA        # Process app_coa table for Washington
lm-dpl process --table soil --state OR       # Process soil data for Oregon
lm-dpl import-file data.shp mytable          # Import shapefile into mytable
lm-dpl --verbose fetch --layer soil OR       # Fetch with verbose logging
        """,
    )

    # Global arguments
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute", required=True
    )

    # Get available layers dynamically from endpoints configuration
    available_layers = get_available_layers()

    # Fetch subcommand
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from remote sources")
    fetch_parser.add_argument(
        "state", help="State name or abbreviation (e.g., OR, WA, oregon, washington)"
    )
    fetch_parser.add_argument(
        "--layer",
        "-l",
        action="append",
        choices=available_layers,
        help=f"Fetch specific layer(s). Available: {', '.join(available_layers)}. Can be used multiple times for multiple layers.",
    )
    fetch_parser.add_argument(
        "--config", help="Path to custom endpoints configuration file"
    )
    fetch_parser.add_argument(
        "--overwrite",
        "-o",
        action="store_true",
        help="Drop and recreate tables before fetching (WARNING: This will delete existing data)",
    )

    # Process subcommand
    process_parser = subparsers.add_parser(
        "process", help="Process fetched data to generate application tables"
    )
    process_parser.add_argument(
        "--table",
        required=True,
        choices=["taxlots", "coa", "soil", "populationpoint"],
        help="Table to process",
    )
    process_parser.add_argument(
        "--state",
        required=True,
        help="State name or abbreviation (e.g., OR, WA, oregon, washington)",
    )

    # Import-file subcommand (unchanged)
    import_parser = subparsers.add_parser("import-file", help="Import data from a file")
    import_parser.add_argument("file_path", help="Path to the file to import")
    import_parser.add_argument("table_name", help="Name of the target table")
    import_parser.add_argument(
        "--srid", type=int, help="Optional source SRID to override file auto-detection"
    )
    import_parser.add_argument(
        "--t-srid",
        type=int,
        dest="t_srid",
        help="Optional target SRID for geometry reprojection",
    )

    args = parser.parse_args()

    # Set up basic logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if args.command == "fetch":
        layers = args.layer  # Can be None or a list
        return run_fetch(
            args.state, layers=layers, config_path=args.config, overwrite=args.overwrite
        )
    elif args.command == "process":
        return run_process(args.table, args.state)
    elif args.command == "import-file":
        return run_import_file(
            args.file_path, args.table_name, None, srid=args.srid, t_srid=args.t_srid
        )
    else:
        # This should not happen due to required=True on subparsers
        logging.error(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
