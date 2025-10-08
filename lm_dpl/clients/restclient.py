#!/usr/bin/env python3
"""
Dynamic REST client for Landmapper data pipeline.

This module provides a dynamic client that reads REST endpoint configurations
from YAML and exposes them as pre-configured RESTFetcher instances.
"""

from typing import Dict, Any, Optional
from pathlib import Path
import requests
import multiprocessing
import json

import yaml
from tqdm import tqdm


def _fetch_data_batch(args):
    """
    Worker function to fetch a single batch of data.
    This is a standalone function to avoid multiprocessing pickling issues.

    Args:
        args: Tuple of (url, params, offset, batch_size)

    Returns:
        list: List of features from this batch
    """
    import time

    url, params, offset, batch_size = args
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            batch_params = params.copy()
            batch_params.update(
                {
                    "resultOffset": offset,
                    "resultRecordCount": batch_size,
                }
            )

            response = requests.get(url, params=batch_params, timeout=60)
            response.raise_for_status()

            # Check if response content is empty before parsing JSON
            if not response.content.strip():
                print(f"Empty response for batch {offset}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return []

            data = response.json()

            if "error" in data:
                print(f"Error in batch {offset}: {data['error']}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return []

            features = data.get("features", [])
            return features

        except requests.exceptions.RequestException as e:
            print(f"Request error fetching batch {offset}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return []
        except ValueError as e:
            # JSON decoding error - likely empty or invalid response
            print(
                f"JSON decode error fetching batch {offset}, attempt {attempt + 1}: {e}"
            )
            print(
                f"Response content: {response.text[:200] if response else 'No response'}"
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return []
        except Exception as e:
            print(
                f"Unexpected error fetching batch {offset}, attempt {attempt + 1}: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return []

    return []


class RESTFetcher:
    """
    A class to fetch data from any ArcGIS REST endpoint.
    """

    def __init__(self, url, params=None):
        """
        Initialize the fetcher with a URL and optional parameters.

        Args:
            url (str): The ArcGIS REST endpoint URL
            params (dict, optional): Default parameters for requests
        """
        self.url = url
        self.default_params = params or {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }

    def get_total_count(self):
        """
        Get the total number of features available.

        Returns:
            int: Total number of features, or None if failed
        """
        try:
            params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}

            response = requests.get(self.url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            return data.get("count", 0)

        except Exception as e:
            print(f"Error getting total count: {e}")
            return None

    def fetch_data(self, output_file=None, batch_size=2000, max_processes=None):
        """
        Fetch data from the ArcGIS REST endpoint using multiprocessing.

        Args:
            output_file (str, optional): Path to save the data
            batch_size (int): Number of records per request (default: 2000)
            max_processes (int, optional): Maximum number of processes to use

        Returns:
            dict: The fetched data as GeoJSON FeatureCollection, or None if failed
        """
        try:
            print(f"Fetching data from: {self.url}")

            # Get total number of features first
            total_count = self.get_total_count()

            if total_count is None:
                print("Failed to get total feature count")
                return None

            print(f"Total features to fetch: {total_count}")

            num_batches = (total_count + batch_size - 1) // batch_size
            print(f"Number of batches: {num_batches}")

            # Prepare arguments for each batch using the standalone function
            batch_args = []
            for i in range(num_batches):
                offset = i * batch_size
                batch_args.append((self.url, self.default_params, offset, batch_size))

            if max_processes is None:
                max_processes = min(multiprocessing.cpu_count(), 8)

            # Use multiprocessing to fetch batches in parallel with progress bar
            with multiprocessing.Pool(processes=max_processes) as pool:
                # Use tqdm to show progress bar for batch downloads
                results = list(
                    tqdm(
                        pool.imap(_fetch_data_batch, batch_args),
                        total=num_batches,
                        desc="Downloading batches",
                        unit="batch",
                    )
                )

            all_features = []
            for features in results:
                all_features.extend(features)

            print(f"Successfully fetched {len(all_features)} total features")

            complete_data = {"type": "FeatureCollection", "features": all_features}

            # Save to file if specified
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                with open(output_path, "w") as f:
                    json.dump(complete_data, f, indent=2)

                print(f"Data saved to: {output_path}")

            return complete_data

        except Exception as e:
            print(f"Error fetching data: {e}")
            return None

    def fetch_with_custom_params(
        self, custom_params, output_file=None, batch_size=2000, max_processes=None
    ):
        """
        Fetch data with custom parameters.

        Args:
            custom_params (dict): Custom parameters for the query
            output_file (str, optional): Path to save the data
            batch_size (int): Number of records per request
            max_processes (int, optional): Maximum number of processes to use

        Returns:
            dict: The fetched data as GeoJSON FeatureCollection, or None if failed
        """
        # Create a temporary fetcher with custom parameters
        temp_fetcher = RESTFetcher(self.url, custom_params)
        return temp_fetcher.fetch_data(output_file, batch_size, max_processes)


class StateService:
    """
    Represents a collection of services for a specific state.

    This class dynamically creates attributes for each service defined
    in the configuration, providing pre-configured RESTFetcher instances
    and access to all service metadata.
    """

    def __init__(self, state_config: Dict[str, Any]):
        """
        Initialize state services from configuration.

        Args:
            state_config: Dictionary containing service configurations for a state
        """
        self._services = {}

        for service_config in state_config:
            for service_name, service_info in service_config.items():
                # Create a RESTFetcher instance for this service
                fetcher = RESTFetcher(
                    url=service_info["url"],
                    params={
                        "where": "1=1",
                        "outFields": "*",
                        "returnGeometry": "true",
                        "f": "geojson",
                    },
                )

                # Store the fetcher and all service metadata
                self._services[service_name] = {
                    "fetcher": fetcher,
                    **service_info  # Include all service info from YAML
                }

                # Create attribute for direct access to fetcher
                setattr(self, service_name, fetcher)

    def get_service_info(self, service_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific service.

        Args:
            service_name: Name of the service

        Returns:
            Dictionary containing all service metadata, or None if not found
        """
        return self._services.get(service_name)

    def list_services(self) -> Dict[str, str]:
        """
        List all available services for this state.

        Returns:
            Dictionary mapping service names to descriptions
        """
        return {name: info["description"] for name, info in self._services.items()}


class LandmapperRESTClient:
    """
    Main client for accessing Landmapper REST endpoints.

    This class dynamically creates state attributes based on the YAML configuration,
    each providing access to pre-configured RESTFetcher instances for that state's services.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the REST client.

        Args:
            config_path: Path to the REST endpoints YAML file. If None, uses default path.
        """
        if config_path is None:
            config_path = Path(__file__).parent / "endpoints.yaml"
        else:
            config_path = Path(config_path)

        self.config_path = config_path
        self._states = {}

        # Load configuration
        self._load_config()

        # Create state attributes
        for state_name, state_config in self._config.items():
            state_service = StateService(state_config)
            self._states[state_name] = state_service
            setattr(self, state_name, state_service)

    def _load_config(self):
        """Load the REST endpoints configuration from YAML."""
        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"REST endpoints configuration file not found: {self.config_path}"
            )
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")

    def get_state_info(self, state_name: str) -> Optional[Dict[str, str]]:
        """
        Get information about services available for a specific state.

        Args:
            state_name: Name of the state (e.g., 'oregon', 'washington')

        Returns:
            Dictionary mapping service names to descriptions, or None if state not found
        """
        state_service = self._states.get(state_name)
        if state_service:
            return state_service.list_services()
        return None

    def list_states(self) -> Dict[str, Dict[str, str]]:
        """
        List all available states and their services.

        Returns:
            Dictionary mapping state names to their service information
        """
        return {
            state_name: state_service.list_services()
            for state_name, state_service in self._states.items()
        }

    def reload_config(self):
        """
        Reload the configuration from the YAML file.

        This is useful if the configuration file has been updated
        and you want to refresh the client without creating a new instance.
        """
        self._load_config()

        # Update state attributes
        for state_name, state_config in self._config.items():
            state_service = StateService(state_config)
            self._states[state_name] = state_service
            setattr(self, state_name, state_service)

        # Remove states that no longer exist in config
        for state_name in list(self._states.keys()):
            if state_name not in self._config:
                del self._states[state_name]
                if hasattr(self, state_name):
                    delattr(self, state_name)
