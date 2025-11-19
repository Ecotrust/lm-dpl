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

        return sorted(list(all_layers))
    except Exception as e:
        logging.warning(f"Could not load layers from endpoints configuration: {e}")
        # Fallback to original hardcoded list
        return ["fpd", "zoning", "plss1", "plss2", "sfd", "taxlots", "coa"]


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


def run_parcels(
    state: Optional[str] = None,
    config_path: Optional[str] = None,
    layers: Optional[list] = None,
    overwrite: bool = False,
) -> int:
    """
    Run parcel processing for the specified state.

    Args:
        state: Optional state name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        config_path: Optional path to custom endpoints configuration file
        layers: Optional list of specific layers to process (e.g., ['plss1', 'plss2'])
        overwrite: If True, drop and recreate tables before processing

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Import here to avoid circular imports
        from lm_dpl.parcels.processor import ParcelProcessor
        import yaml

        # Determine state: use CLI state if provided, otherwise extract from config
        if config_path:
            # Load config to extract state
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)

            # Extract state from top-level keys
            config_states = list(config.keys())
            if not config_states:
                raise ValueError(f"No state found in config file: {config_path}")

            config_state = config_states[0]  # Use first state found

            # Use CLI state if provided, otherwise use config state
            if state:
                normalized_state = normalize_state(state)
                logging.info(
                    f"Using state from command line: {normalized_state} (overriding config state: {config_state})"
                )
            else:
                normalized_state = normalize_state(config_state)
                logging.info(f"Using state from config file: {normalized_state}")
        else:
            # No config file, state is required
            if not state:
                raise ValueError(
                    "State argument is required when no config file is provided"
                )
            normalized_state = normalize_state(state)
            logging.info(f"Using state from command line: {normalized_state}")

        processor = ParcelProcessor(normalized_state, config_path=config_path)

        # Process only specified layers
        if layers:
            for layer in layers:
                try:
                    processor.process_service(layer, overwrite=overwrite)
                except Exception as e:
                    logging.error(
                        f"Error processing layer '{layer}' for state {normalized_state}: {e}"
                    )
                    continue
        else:
            # Process all layers
            processor.fetch(overwrite=overwrite)

        return 0

    except Exception as e:
        logging.error(f"Error processing parcels: {e}")
        return 1


def run_app_taxlot(state: str, config_path: Optional[str] = None) -> int:
    """
    Run app_taxlot table processing for the specified state.

    Args:
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        config_path: Optional path to configuration file

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Import here to avoid circular imports
        from lm_dpl.parcels.processor import ParcelProcessor

        normalized_state = normalize_state(state)
        processor = ParcelProcessor(normalized_state)
        processor.process_app_taxlot()

        return 0

    except Exception as e:
        logging.error(f"Error processing app_taxlot for state {state}: {e}")
        return 1


def run_app_coa(state: str, config_path: Optional[str] = None) -> int:
    """
    Run app_coa table processing for the specified state.

    Args:
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        config_path: Optional path to configuration file

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Import here to avoid circular imports
        from lm_dpl.parcels.processor import ParcelProcessor

        normalized_state = normalize_state(state)
        processor = ParcelProcessor(normalized_state)
        processor.process_app_coa()

        return 0

    except Exception as e:
        logging.error(f"Error processing app_coa for state {state}: {e}")
        return 1


def run_app_populationpoint(state: str, config_path: Optional[str] = None) -> int:
    """
    Run app_populationpoint table processing for the specified state.

    Args:
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        config_path: Optional path to configuration file

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Import here to avoid circular imports
        from lm_dpl.parcels.processor import ParcelProcessor

        normalized_state = normalize_state(state)
        processor = ParcelProcessor(normalized_state)
        processor.process_app_populationpoint()

        return 0

    except Exception as e:
        logging.error(f"Error processing app_populationpoint for state {state}: {e}")
        return 1


def run_soil(state: str, config_path: Optional[str] = None) -> int:
    """
    Run soil processing for the specified state.

    Args:
        state: State name or abbreviation (e.g., 'oregon', 'OR', 'washington', 'WA')
        config_path: Optional path to configuration file

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        # Import here to avoid circular imports
        from lm_dpl.soil.processor import main as soil_main

        normalized_state = normalize_state(state, to="abbr")

        soil_main(normalized_state, config_path)
        return 0

    except Exception as e:
        logging.error(f"Error processing soil data for state {state}: {e}")
        return 1


def run_import_file(
    file_path: str, table_name: str, config_path: Optional[str] = None
) -> int:
    """
    Import data from a file into the database.

    Args:
        file_path: Path to the file to import.
        table_name: Name of the target table.
        config_path: Optional path to configuration file.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        config = get_config()
        db_creds = config.postgres_dsn_dict
        import_from_file(db_creds, file_path, table_name)
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
lm-dpl parcels --config config.yml           # Process parcels with state from config file
lm-dpl parcels --config config.yml oregon    # Process parcels for Oregon with custom config (override)
lm-dpl parcels oregon                        # Process parcels for Oregon with default config
lm-dpl --verbose soil OR                     # Process soil data for Oregon with verbose logging
lm-dpl parcels washington                    # Process parcels for Washington
lm-dpl soil WA                               # Process soil data for Washington
lm-dpl parcels --layer fpd oregon            # Process only FPD layer for Oregon
lm-dpl parcels --layer fpd --layer plss1 oregon  # Process FPD and PLSS1 layers for Oregon
lm-dpl parcels -l fpd -l plss2 oregon        # Process FPD and PLSS2 layers for Oregon (short form)
        """,
    )

    # Global arguments
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute", required=True
    )

    parcels_parser = subparsers.add_parser("parcels", help="Process parcel data")
    parcels_parser.add_argument(
        "state",
        nargs="?",
        help="State name or abbreviation (e.g., oregon, OR, washington, WA). Optional when using --config",
    )
    parcels_parser.add_argument(
        "--config", help="Path to custom endpoints configuration file"
    )

    # Get available layers dynamically from endpoints configuration
    available_layers = get_available_layers()

    parcels_parser.add_argument(
        "--layer",
        "-l",
        action="append",
        choices=available_layers,
        help=f"Process specific layer(s). Available: {', '.join(available_layers)}. Can be used multiple times for multiple layers.",
    )
    parcels_parser.add_argument(
        "--overwrite",
        "-o",
        action="store_true",
        help="Drop and recreate tables before processing (WARNING: This will delete existing data)",
    )

    soil_parser = subparsers.add_parser("soil", help="Process soil data")
    soil_parser.add_argument(
        "state", help="State name or abbreviation (e.g., oregon, OR, washington, WA)"
    )

    import_parser = subparsers.add_parser("import-file", help="Import data from a file")
    import_parser.add_argument("file_path", help="Path to the file to import")
    import_parser.add_argument("table_name", help="Name of the target table")

    # Add new subparsers for app table processing
    app_taxlot_parser = subparsers.add_parser(
        "app-taxlot", help="Process app_taxlot table"
    )
    app_taxlot_parser.add_argument(
        "state", help="State name or abbreviation (e.g., oregon, OR, washington, WA)"
    )

    app_coa_parser = subparsers.add_parser("app-coa", help="Process app_coa table")
    app_coa_parser.add_argument(
        "state", help="State name or abbreviation (e.g., oregon, OR, washington, WA)"
    )

    app_populationpoint_parser = subparsers.add_parser(
        "app-populationpoint", help="Process app_populationpoint table"
    )
    app_populationpoint_parser.add_argument(
        "state", help="State name or abbreviation (e.g., oregon, OR, washington, WA)"
    )

    args = parser.parse_args()

    # Set up basic logging (will be overridden in command functions)
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if args.command == "parcels":
        layers = args.layer if args.layer else None
        return run_parcels(args.state, args.config, layers, overwrite=args.overwrite)
    elif args.command == "soil":
        return run_soil(args.state, None)
    elif args.command == "import-file":
        return run_import_file(args.file_path, args.table_name, None)
    elif args.command == "app-taxlot":
        return run_app_taxlot(args.state, None)
    elif args.command == "app-coa":
        return run_app_coa(args.state, None)
    elif args.command == "app-populationpoint":
        return run_app_populationpoint(args.state, None)
    else:
        # This should not happen due to required=True on subparsers
        logging.error(f"Unknown command: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
