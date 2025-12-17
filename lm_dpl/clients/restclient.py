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
import time

import yaml
from tqdm import tqdm


def _fetch_data_batch(args):
    """
    Worker function to fetch a single batch of data.
    This is a standalone function to avoid multiprocessing pickling issues.

    Args:
        args: Tuple of (url, params, offset, batch_size)

    Returns:
        dict: Dictionary containing batch result with status and data
    """
    import time
    import random

    url, params, offset, batch_size = args
    max_retries = 5  # Increased from 3 to 5
    base_delay = 2  # seconds
    max_delay = 60  # Maximum delay for exponential backoff

    for attempt in range(max_retries):
        try:
            batch_params = params.copy()
            batch_params.update(
                {
                    "resultOffset": offset,
                    "resultRecordCount": batch_size,
                }
            )

            # Add jitter to avoid synchronized retries
            delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
            
            # Add delay between retries (except first attempt)
            if attempt > 0:
                print(f"Waiting {delay:.2f}s before retry {attempt + 1} for batch {offset}")
                time.sleep(delay)

            response = requests.get(url, params=batch_params, timeout=120)  # Increased timeout
            response.raise_for_status()

            # Check if response content is empty before parsing JSON
            if not response.content.strip():
                print(f"Empty response for batch {offset}, attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue
                return {
                    "status": "failed",
                    "offset": offset,
                    "batch_size": batch_size,
                    "attempts": attempt + 1,
                    "error": "Empty response",
                    "features": []
                }

            data = response.json()

            if "error" in data:
                print(f"Error in batch {offset}: {data['error']}")
                if attempt < max_retries - 1:
                    continue
                return {
                    "status": "failed",
                    "offset": offset,
                    "batch_size": batch_size,
                    "attempts": attempt + 1,
                    "error": f"Service error: {data['error']}",
                    "features": []
                }

            features = data.get("features", [])
            
            # Add small delay between successful requests to avoid rate limiting
            if features:
                time.sleep(0.5 + random.uniform(0, 0.5))  # 0.5-1.0 second delay
            
            return {
                "status": "success",
                "offset": offset,
                "batch_size": batch_size,
                "attempts": attempt + 1,
                "error": None,
                "features": features
            }

        except requests.exceptions.RequestException as e:
            print(f"Request error fetching batch {offset}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                continue
            return {
                "status": "failed",
                "offset": offset,
                "batch_size": batch_size,
                "attempts": attempt + 1,
                "error": f"Request error: {str(e)}",
                "features": []
            }
        except ValueError as e:
            # JSON decoding error - likely empty or invalid response
            print(
                f"JSON decode error fetching batch {offset}, attempt {attempt + 1}: {e}"
            )
            print(
                f"Response content: {response.text[:200] if response else 'No response'}"
            )
            if attempt < max_retries - 1:
                continue
            return {
                "status": "failed",
                "offset": offset,
                "batch_size": batch_size,
                "attempts": attempt + 1,
                "error": f"JSON decode error: {str(e)}",
                "features": []
            }
        except Exception as e:
            print(
                f"Unexpected error fetching batch {offset}, attempt {attempt + 1}: {e}"
            )
            if attempt < max_retries - 1:
                continue
            return {
                "status": "failed",
                "offset": offset,
                "batch_size": batch_size,
                "attempts": attempt + 1,
                "error": f"Unexpected error: {str(e)}",
                "features": []
            }

    return {
        "status": "failed",
        "offset": offset,
        "batch_size": batch_size,
        "attempts": max_retries,
        "error": "Max retries exceeded",
        "features": []
    }


class RESTFetcher:
    """
    A class to fetch data from any ArcGIS REST endpoint.
    """

    def __init__(self, url, params=None, where=None, max_processes=None):
        """
        Initialize the fetcher with a URL and optional parameters.

        Args:
            url (str): The ArcGIS REST endpoint URL
            params (dict, optional): Default parameters for requests
            where (str, optional): WHERE clause for filtering features. If None, defaults to "1=1"
            max_processes (int, optional): Maximum number of processes to use for this service
        """
        self.url = url
        self.where = where or "1=1"
        self.default_params = params or {
            "where": self.where,
            "outFields": "*",
            "returnGeometry": "true",
            "f": "geojson",
        }
        self.max_processes = max_processes

    def get_total_count(self):
        """
        Get the total number of features available.

        Returns:
            int: Total number of features, or None if failed
        """
        try:
            params = {"where": self.where, "returnCountOnly": "true", "f": "json"}

            response = requests.get(self.url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            return data.get("count", 0)

        except Exception as e:
            print(f"Error getting total count: {e}")
            return None

    def fetch_data(
        self, output_file=None, epsg=None, batch_size=2000, max_processes=None
    ):
        """
        Fetch data from the ArcGIS REST endpoint using multiprocessing.

        Args:
            output_file (str, optional): Path to save the data
            epsg (int, optional): EPSG code for output spatial reference
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

            params = self.default_params.copy()
            if epsg:
                params["outSR"] = epsg

            # Prepare arguments for each batch using the standalone function
            batch_args = []
            for i in range(num_batches):
                offset = i * batch_size
                batch_args.append((self.url, params, offset, batch_size))

            if max_processes is None:
                # Use service-specific max_processes if configured, otherwise use default
                if self.max_processes is not None:
                    max_processes = self.max_processes
                    print(f"Using configured max_processes: {max_processes}")
                else:
                    # Default fallback logic
                    max_processes = min(multiprocessing.cpu_count(), 4)

            print(f"Using {max_processes} concurrent processes")

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

            # Process results and identify failed batches
            all_features = []
            failed_batches = []
            successful_batches = 0

            for batch_result in results:
                if batch_result["status"] == "success":
                    all_features.extend(batch_result["features"])
                    successful_batches += 1
                else:
                    failed_batches.append(batch_result)
                    print(f"Failed batch {batch_result['offset']}: {batch_result['error']}")

            print(f"Successfully fetched {len(all_features)} features from {successful_batches} batches")

            # Automatically retry failed batches
            persistent_failures = []
            if failed_batches:
                print(f"\nRetrying {len(failed_batches)} failed batches...")
                retry_args = [(self.url, params, batch["offset"], batch["batch_size"]) for batch in failed_batches]
                
                with multiprocessing.Pool(processes=min(max_processes, len(retry_args))) as pool:
                    retry_results = list(
                        tqdm(
                            pool.imap(_fetch_data_batch, retry_args),
                            total=len(retry_args),
                            desc="Retrying failed batches",
                            unit="batch",
                        )
                    )

                # Process retry results
                retry_successful = 0
                for batch_result in retry_results:
                    if batch_result["status"] == "success":
                        all_features.extend(batch_result["features"])
                        retry_successful += 1
                    else:
                        persistent_failures.append(batch_result)
                        print(f"Batch {batch_result['offset']} still failed after retry: {batch_result['error']}")

                print(f"Successfully recovered {retry_successful} batches from retry")

            # Log persistent failures for manual recovery
            if persistent_failures:
                failure_log = {
                    "timestamp": time.time(),
                    "service_url": self.url,
                    "total_batches": num_batches,
                    "successful_batches": successful_batches + retry_successful,
                    "persistent_failures": persistent_failures,
                    "params": params
                }
                
                # Save failure log to file
                failure_log_path = Path(f"failed_batches_{int(time.time())}.json")
                with open(failure_log_path, "w") as f:
                    json.dump(failure_log, f, indent=2)
                
                print(f"Persistent failures logged to: {failure_log_path}")
                print(f"Use retry_failed_batches() method to recover these batches later")

            print(f"Final result: {len(all_features)} total features from {successful_batches + (retry_successful if failed_batches else 0)} batches")

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

    def retry_failed_batches(self, failure_log_file, max_processes=None, output_file=None):
        """
        Retry failed batches from a previous failure log.

        Args:
            failure_log_file (str): Path to the failure log JSON file
            max_processes (int, optional): Maximum number of processes to use
            output_file (str, optional): Path to save the recovered data

        Returns:
            dict: The recovered data as GeoJSON FeatureCollection, or None if failed
        """
        try:
            # Load failure log
            with open(failure_log_file, "r") as f:
                failure_log = json.load(f)

            print(f"Retrying {len(failure_log['persistent_failures'])} failed batches from {failure_log_file}")

            # Prepare arguments for retrying failed batches
            retry_args = []
            for failed_batch in failure_log["persistent_failures"]:
                retry_args.append((
                    self.url,
                    failure_log["params"],
                    failed_batch["offset"],
                    failed_batch["batch_size"]
                ))

            if max_processes is None:
                max_processes = min(multiprocessing.cpu_count(), 4)

            print(f"Using {max_processes} concurrent processes for recovery")

            # Retry failed batches
            with multiprocessing.Pool(processes=min(max_processes, len(retry_args))) as pool:
                retry_results = list(
                    tqdm(
                        pool.imap(_fetch_data_batch, retry_args),
                        total=len(retry_args),
                        desc="Recovering failed batches",
                        unit="batch",
                    )
                )

            # Process retry results
            recovered_features = []
            successful_recoveries = 0
            still_failed = []

            for batch_result in retry_results:
                if batch_result["status"] == "success":
                    recovered_features.extend(batch_result["features"])
                    successful_recoveries += 1
                else:
                    still_failed.append(batch_result)
                    print(f"Batch {batch_result['offset']} still failed: {batch_result['error']}")

            print(f"Successfully recovered {successful_recoveries} batches")
            if still_failed:
                print(f"{len(still_failed)} batches still failed after recovery attempt")

            # Create result data
            recovered_data = {"type": "FeatureCollection", "features": recovered_features}

            # Save to file if specified
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                with open(output_path, "w") as f:
                    json.dump(recovered_data, f, indent=2)

                print(f"Recovered data saved to: {output_path}")

            return recovered_data

        except FileNotFoundError:
            print(f"Failure log file not found: {failure_log_file}")
            return None
        except json.JSONDecodeError:
            print(f"Invalid failure log file: {failure_log_file}")
            return None
        except Exception as e:
            print(f"Error during batch recovery: {e}")
            return None


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
                # Extract where clause from service info, default to None if not specified
                where_clause = service_info.get("where")
                
                # Extract max_processes from service info if specified
                max_processes = service_info.get("max_processes")
                
                # Create a RESTFetcher instance for this service
                fetcher = RESTFetcher(
                    url=service_info["url"],
                    params={
                        "where": where_clause or "1=1",
                        "outFields": "*",
                        "returnGeometry": "true",
                        "f": "geojson",
                    },
                    where=where_clause,
                    max_processes=max_processes,
                )

                # Store the fetcher and all service metadata
                self._services[service_name] = {
                    "fetcher": fetcher,
                    **service_info,  # Include all service info from YAML
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

    def test_endpoints(self, timeout: int = 30, state: Optional[str] = None) -> Dict[str, Any]:
        """
        Test connectivity and response validation for REST endpoints.

        Args:
            timeout: Request timeout in seconds (default: 30)
            state: Optional state name to test only endpoints for that specific state.
                   If None, tests all states (default: None)

        Returns:
            Dictionary containing test results with the following structure:
            {
                "summary": {
                    "total_endpoints": int,
                    "successful": int,
                    "failed": int,
                    "success_rate": float
                },
                "results": {
                    "state_name": {
                        "service_name": {
                            "status": "success" | "error",
                            "status_code": int,
                            "response_time": float,
                            "error": str | None,
                            "url": str
                        }
                    }
                }
            }

        Raises:
            ValueError: If the specified state does not exist in the configuration
        """
        # Validate state parameter if provided
        if state is not None:
            if state not in self._states:
                raise ValueError(f"State '{state}' not found. Available states: {list(self._states.keys())}")
            states_to_test = {state: self._states[state]}
        else:
            states_to_test = self._states

        results = {}
        total_endpoints = 0
        successful = 0
        failed = 0

        print("Testing REST endpoints connectivity...")

        for state_name, state_service in states_to_test.items():
            state_results = {}
            services = state_service.list_services()

            for service_name, service_description in services.items():
                total_endpoints += 1
                service_info = state_service.get_service_info(service_name)
                url = service_info["url"]

                try:
                    # Test basic connectivity with a minimal query
                    test_params = {
                        "where": "1=1",
                        "returnCountOnly": "true",
                        "f": "json",
                    }

                    start_time = time.time()
                    response = requests.get(url, params=test_params, timeout=timeout)
                    response_time = time.time() - start_time

                    response.raise_for_status()

                    # Validate response structure
                    data = response.json()
                    if "count" in data or "features" in data or "error" not in data:
                        state_results[service_name] = {
                            "status": "success",
                            "status_code": response.status_code,
                            "response_time": round(response_time, 3),
                            "error": None,
                            "url": url,
                        }
                        successful += 1
                        print(
                            f"✓ {state_name}.{service_name}: SUCCESS ({response_time:.3f}s)"
                        )
                    else:
                        state_results[service_name] = {
                            "status": "error",
                            "status_code": response.status_code,
                            "response_time": round(response_time, 3),
                            "error": f"Invalid response structure: {data}",
                            "url": url,
                        }
                        failed += 1
                        print(
                            f"✗ {state_name}.{service_name}: ERROR - Invalid response structure"
                        )

                except requests.exceptions.Timeout:
                    state_results[service_name] = {
                        "status": "error",
                        "status_code": None,
                        "response_time": timeout,
                        "error": f"Request timeout after {timeout} seconds",
                        "url": url,
                    }
                    failed += 1
                    print(f"✗ {state_name}.{service_name}: ERROR - Timeout")

                except requests.exceptions.ConnectionError:
                    state_results[service_name] = {
                        "status": "error",
                        "status_code": None,
                        "response_time": 0,
                        "error": "Connection failed - endpoint unreachable",
                        "url": url,
                    }
                    failed += 1
                    print(f"✗ {state_name}.{service_name}: ERROR - Connection failed")

                except requests.exceptions.HTTPError as e:
                    state_results[service_name] = {
                        "status": "error",
                        "status_code": (
                            response.status_code if "response" in locals() else None
                        ),
                        "response_time": (
                            round(response_time, 3)
                            if "response_time" in locals()
                            else 0
                        ),
                        "error": f"HTTP Error: {str(e)}",
                        "url": url,
                    }
                    failed += 1
                    print(
                        f"✗ {state_name}.{service_name}: ERROR - HTTP {response.status_code if 'response' in locals() else 'Unknown'}"
                    )

                except Exception as e:
                    state_results[service_name] = {
                        "status": "error",
                        "status_code": None,
                        "response_time": 0,
                        "error": f"Unexpected error: {str(e)}",
                        "url": url,
                    }
                    failed += 1
                    print(f"✗ {state_name}.{service_name}: ERROR - {str(e)}")

            results[state_name] = state_results

        # Calculate success rate
        success_rate = (
            (successful / total_endpoints) * 100 if total_endpoints > 0 else 0
        )

        summary = {
            "total_endpoints": total_endpoints,
            "successful": successful,
            "failed": failed,
            "success_rate": round(success_rate, 2),
        }

        print(f"\nTest Summary:")
        print(f"  Total endpoints: {total_endpoints}")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Success rate: {success_rate:.1f}%")

        return {"summary": summary, "results": results}

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
