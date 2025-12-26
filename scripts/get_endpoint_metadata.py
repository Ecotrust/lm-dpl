#!/usr/bin/env python3
"""
Script to verify endpoints and extract metadata including:
- Field names
- EPSG/spatial reference
- Data types
- Max records capability

Outputs results in the same format as lm_dpl/clients/endpoints.yaml
"""

import argparse
import yaml
import requests
from typing import Dict, List, Any, Optional


def get_service_info(url: str) -> Optional[Dict[str, Any]]:
    """
    Get service information from ArcGIS REST endpoint.

    Args:
        url: The ArcGIS REST endpoint URL

    Returns:
        Dictionary containing service metadata or None if failed
    """
    try:
        # Remove /query from URL to get service info
        service_url = url.replace("/query", "")

        # Add parameters to get full service info
        params = {"f": "json"}

        response = requests.get(service_url, params=params, timeout=30)
        response.raise_for_status()

        return response.json()

    except Exception as e:
        print(f"Error getting service info for {url}: {e}")
        return None


def extract_field_info(fields: List[Dict]) -> tuple:
    """
    Extract field names and data types from ArcGIS field definitions.

    Args:
        fields: List of field definitions from ArcGIS service

    Returns:
        Tuple of (field_names, data_types)
    """
    field_names = []
    data_types = []

    for field in fields:
        name = field.get("name", "")
        field_type = field.get("type", "")

        # Map ArcGIS field types to SQL-like types
        type_mapping = {
            "esriFieldTypeString": "VARCHAR(255)",
            "esriFieldTypeSmallInteger": "SMALLINT",
            "esriFieldTypeInteger": "INTEGER",
            "esriFieldTypeDouble": "FLOAT",
            "esriFieldTypeSingle": "FLOAT",
            "esriFieldTypeDate": "DATE",
            "esriFieldTypeOID": "BIGINT",
            "esriFieldTypeGeometry": "GEOMETRY",
            "esriFieldTypeBlob": "BYTEA",
            "esriFieldTypeRaster": "BYTEA",
            "esriFieldTypeGUID": "VARCHAR(38)",
            "esriFieldTypeGlobalID": "VARCHAR(38)",
            "esriFieldTypeXML": "TEXT",
        }

        sql_type = type_mapping.get(field_type, "VARCHAR(255)")

        # Handle string length for VARCHAR types
        if sql_type.startswith("VARCHAR") and "length" in field:
            length = field.get("length", 255)
            sql_type = f"VARCHAR({length})"

        field_names.append(name)
        data_types.append(sql_type)

    return field_names, data_types


def get_epsg_from_spatial_reference(spatial_reference: Dict) -> Optional[int]:
    """
    Extract EPSG code from ArcGIS spatial reference.

    Args:
        spatial_reference: Spatial reference dictionary from ArcGIS service

    Returns:
        EPSG code or None if not found
    """
    try:
        # Try to get WKID (Well-Known ID)
        wkid = spatial_reference.get("wkid")
        if wkid:
            return int(wkid)

        # Try to get latest WKID
        latest_wkid = spatial_reference.get("latestWkid")
        if latest_wkid:
            return int(latest_wkid)

    except (ValueError, TypeError):
        pass

    return None


def get_max_records(service_info: Dict) -> int:
    """
    Determine max records from service capabilities.

    Args:
        service_info: Service metadata dictionary

    Returns:
        Maximum records per request
    """
    try:
        # Check for maxRecordCount in service info
        max_record_count = service_info.get("maxRecordCount")
        if max_record_count:
            return min(max_record_count, 10000)  # Cap at reasonable limit

        # Check for advancedQueryCapabilities
        query_caps = service_info.get("advancedQueryCapabilities", {})
        max_records = query_caps.get("maxRecordCount")
        if max_records:
            return min(max_records, 10000)

    except (AttributeError, TypeError):
        pass

    # Default fallback
    return 2000


def read_urls_from_file(urls_file: str) -> List[str]:
    """
    Read URLs from a text file (one URL per line).

    Args:
        urls_file: Path to the text file containing URLs

    Returns:
        List of URLs
    """
    urls = []
    try:
        with open(urls_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):  # Skip empty lines and comments
                    urls.append(line)
        return urls
    except Exception as e:
        print(f"Error reading URLs from file {urls_file}: {e}")
        return []


def generate_endpoint_name(url: str) -> str:
    """
    Generate a meaningful endpoint name from URL.

    Args:
        url: The endpoint URL

    Returns:
        Generated endpoint name
    """
    try:
        # Extract the last part of the URL path as service name
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split("/") if p]
        
        if path_parts:
            # Use the last non-empty path component
            service_name = path_parts[-1]
            # Clean up the name
            service_name = service_name.replace("_", " ").replace("-", " ").title()
            service_name = service_name.replace(" ", "_").lower()
            return service_name
    except Exception:
        pass
    
    # Fallback: use sequential naming
    return f"endpoint_{hash(url) % 10000}"


def verify_endpoints_from_urls(urls: List[str]) -> Dict[str, Any]:
    """
    Verify endpoints and extract metadata from a list of URLs.

    Args:
        urls: List of endpoint URLs to verify

    Returns:
        Dictionary containing verification results
    """
    results = {"endpoints": []}

    print(f"Verifying {len(urls)} endpoints...")

    for i, url in enumerate(urls, 1):
        endpoint_name = generate_endpoint_name(url)
        print(f"\nProcessing {endpoint_name} ({i}/{len(urls)})...")

        # Get service information
        service_data = get_service_info(url)

        if not service_data:
            print(f"  ✗ Failed to get service info for {endpoint_name}")
            continue

        # Extract field information
        fields = service_data.get("fields", [])
        field_names, data_types = extract_field_info(fields)

        # Extract EPSG
        spatial_ref = service_data.get("spatialReference", {})
        epsg = get_epsg_from_spatial_reference(spatial_ref)

        # Get max records
        max_records = get_max_records(service_data)

        # Create service configuration
        service_config = {
            endpoint_name: {
                "description": f"ArcGIS service: {endpoint_name}",
                "url": url,
                "fetch": True,
                "geom": True,
                "max_records": max_records,
            }
        }

        # Add optional fields if available
        if epsg:
            service_config[endpoint_name]["epsg"] = epsg

        if field_names:
            service_config[endpoint_name]["outfields"] = ",".join(field_names)

        if data_types:
            service_config[endpoint_name]["dtypes"] = ",".join(data_types)

        results["endpoints"].append(service_config)
        print(f"  ✓ Successfully processed {endpoint_name}")
        print(f"    - Fields: {len(field_names)}")
        print(f"    - EPSG: {epsg}")
        print(f"    - Max records: {max_records}")

    return results


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Verify endpoints and extract metadata from ArcGIS REST services"
    )
    parser.add_argument(
        "--urls", 
        required=True,
        help="Path to text file containing endpoint URLs (one per line)"
    )
    parser.add_argument(
        "--output", 
        required=True,
        help="Name of the output YAML file to generate"
    )
    return parser.parse_args()


def main():
    """Main function to verify endpoints and save results."""
    args = parse_arguments()

    print(f"Reading URLs from: {args.urls}")

    # Read URLs from file
    urls = read_urls_from_file(args.urls)
    
    if not urls:
        print("No valid URLs found in the file.")
        return

    print(f"Found {len(urls)} URLs to process")

    # Verify endpoints
    results = verify_endpoints_from_urls(urls)

    # Save results
    with open(args.output, "w") as f:
        yaml.dump(results, f, default_flow_style=False, indent=2)

    print(f"\nResults saved to: {args.output}")

    # Print summary
    total_endpoints = len(results["endpoints"])
    print(f"\nSummary:")
    print(f"  Total endpoints processed: {total_endpoints}")
    print(f"  Output file: {args.output}")


if __name__ == "__main__":
    main()
