"""
Parcel Data Processor - A module to fetch data from REST endpoints and
ingest into PostGIS database.

Reads endpoint configurations from YAML files, fetches data using the REST client,
and inserts data into PostGIS database.
"""

import json
from typing import Dict, Any

from ..clients import LandmapperRESTClient, DatabaseManager
from ..utils import get_config, import_layer, get_project_logger


class ParcelProcessor:
    """Class to process parcel data from REST endpoints into PostGIS database."""

    def __init__(self, state: str):
        """Initialize the processor for a specific state.

        Args:
            state: State name ('oregon' or 'washington')
        """
        self.logger = get_project_logger(__name__)
        self.state = state.lower()
        self.rest_client = LandmapperRESTClient()
        self.config = get_config()

        # Get database credentials from config
        self.db_credentials = self.config.postgres_dsn_dict

        # Get state services
        self.state_services = getattr(self.rest_client, self.state, None)
        if self.state_services is None:
            raise ValueError(f"State '{state}' not found in REST client configuration")

    def _create_table(self, service_name: str, service_info: Dict[str, Any]) -> None:
        """Create a PostGIS table for a service if it doesn't exist.

        Args:
            service_name: Name of the service (e.g., 'fpd', 'zonning')
            service_info: Service configuration from endpoints.yaml
        """
        table_name = f"s_{self.state}_{service_name}"

        # Parse outfields and dtypes
        outfields = service_info.get("outfields", "").split(",")
        dtypes = service_info.get("dtypes", "").split(",")

        if len(outfields) != len(dtypes):
            raise ValueError(f"Outfields and dtypes count mismatch for {service_name}")

        # Build column definitions
        columns_def = []
        for field, dtype in zip(outfields, dtypes):
            columns_def.append(f"{field.strip()} {dtype.strip()}")

        # Add geometry column if service has geometry
        if service_info.get("geom", False):
            epsg = service_info.get("epsg", 4326)
            columns_def.append(f"geom GEOMETRY(GEOMETRY, {epsg})")

        # Create table SQL
        create_table_sql = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns_def)}
            );
        """

        with DatabaseManager(self.db_credentials) as db:
            db.execute(create_table_sql)

    def process_service(self, service_name: str) -> None:
        """Process a single service: fetch data and insert into database.

        Args:
            service_name: Name of the service to process
        """
        self.logger.info(f"Processing service: {service_name}")

        service_info = self.state_services.get_service_info(service_name)
        if not service_info:
            self.logger.warning(
                f"Service '{service_name}' not found for state '{self.state}'"
            )
            return

        # Skip services with fetch: false
        if not service_info.get("fetch", True):
            self.logger.info(f"Skipping service '{service_name}' (fetch: false)")
            return

        # Create table
        self._create_table(service_name, service_info)

        # Get fetcher for this service
        fetcher = getattr(self.state_services, service_name, None)
        if not fetcher:
            self.logger.warning(f"Fetcher not available for service '{service_name}'")
            return

        try:
            self.logger.info(f"Fetching data for {service_name}")
            batch_size = service_info.get("max_records", 2000)
            data = fetcher.fetch_data(batch_size=batch_size)

            if not data or "features" not in data or not data["features"]:
                self.logger.warning(f"No data returned for service '{service_name}'")
                return

            table_name = f"s_{self.state}_{service_name}"
            outfields = service_info.get("outfields", "").split(",")
            property_keys = [field.strip() for field in outfields]
            columns = property_keys.copy()

            has_geometry = service_info.get("geom", False)
            if has_geometry:
                columns.append("geom")

            data_rows = []
            for feature in data["features"]:
                properties = feature.get("properties", {})
                row = []

                for key in property_keys:
                    row.append(properties.get(key))

                if has_geometry:
                    geometry = feature.get("geometry", {})
                    row.append(json.dumps(geometry) if geometry else None)

                data_rows.append(row)

            # Insert data into database
            if data_rows:
                import_layer(
                    db_credentials=self.db_credentials,
                    data=data_rows,
                    table_name=table_name,
                    columns=columns,
                    property_keys=property_keys,
                    srid=service_info.get("epsg", 4326) if has_geometry else None,
                    has_geometry=has_geometry,
                    geom_type="geojson" if has_geometry else None,
                    num_threads=4,
                )
                self.logger.info(
                    f"Successfully processed {len(data_rows)} records for {service_name}"
                )
            else:
                self.logger.warning(f"No valid data to insert for {service_name}")

        except Exception as e:
            self.logger.error(f"Failed to process service '{service_name}': {e}")
            raise

    def run(self) -> None:
        """Run the complete processing pipeline for all services in the state."""
        self.logger.info(f"Starting parcel processor for state: {self.state}")

        # Get all services for the state
        services = self.state_services.list_services()

        for service_name in services.keys():
            try:
                self.process_service(service_name)
            except Exception as e:
                self.logger.error(f"Failed to process service '{service_name}': {e}")
                continue

        self.logger.info(f"Completed parcel processing for state: {self.state}")


def main(state: str, config_path: str = None) -> None:
    """
    Main entry point for parcel processing from CLI.

    Args:
        state: State name (e.g., 'oregon', 'washington')
        config_path: Optional path to configuration file (currently unused)
    """
    from ..utils.logging_utils import setup_project_logging

    logger = setup_project_logging()
    logger.info(f"Starting parcel processing for state: {state}")

    try:
        processor = ParcelProcessor(state)
        processor.run()
        logger.info(f"Completed parcel processing for state: {state}")
    except Exception as e:
        logger.error(f"Error during parcel processing for state {state}: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    import json

    processor = ParcelProcessor("oregon")
    processor.process_service("zonning")
