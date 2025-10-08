import json
import requests


class SDADataQ:
    """NRCS Soil Data Mart Data Access."""

    url = "https://sdmdataaccess.nrcs.usda.gov/tabular/post.rest"

    def run_query(self, query):
        """
        Sends a query to the Soil Data Access web service.

        Args:
            query (str): The SQL query to execute.
        Returns:
            List of dictionaries representing the query results.
        """
        headers = {"Content-Type": "application/json"}
        payload = {"query": query, "format": "json"}
        response = requests.post(self.url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            data = response.json()
            if len(data.get("Table", [])) > 0:
                return data["Table"]
            else:
                print("No data returned from the tabular query.")
                return None
        else:
            print(f"Error from tabular service: {response.status_code}")
            print(response.text)
            return None


class SDASpatialQ:
    """NRCS Soil Data Mart Data Access Web Feature Servicec
     Provides NRCS SSURGO Soils data using the standard Web Feature Service (WFS) protocol,
     with coordinates in the Web Mercator projection.

     Keywords: NRCS, Soils, SSURGO, Mapunit
     Contact: Steve Peaslee (GIS Specialist, steve.peaslee@lin.usda.gov)

    Available Operations
     * GetCapabilities: Get a description of the service's capabilities (this document).
     * DescribeFeatureType: Get the data structure (schema) for a specific layer.
     * GetFeature: Retrieve the actual data for a layer.

    Available Data Layers (FeatureType)
     * `mapunitpoly`: Soil Mapunit Polygons.
     * `mapunitline`: Soil Mapunit Lines.
     * `mapunitpoint`: Soil Mapunit Points.
     * `mapunitpolyextended`: Soil Mapunit Polygons with extra attributes.
     * `mapunitlineextended`: Soil Mapunit Lines with extra attributes.
     * `mapunitpointextended`: Soil Mapunit Points with extra attributes.
     * `mapunitpolythematic`: Thematic map of soil polygons based on a selected interpretation.
     * `mapunitlinethematic`: Thematic map of soil lines.
     * `mapunitpointthematic`: Thematic map of soil points.
     * `surveyareapoly`: Polygons showing the status of SSURGO soil survey areas.
     * `surveyareapolytransparent`: Same as above, but with a semi-transparent fill.
     * `surveyareapolyoutline`: Outline-only version of the soil survey areas.
     * `featline`: Lines representing various landscape features.
     * `featpoint`: Points representing various landscape features.
     * `aoi`: Boundary of a user-defined Area of Interest.
     * `aoihatched`: Crosshatched boundary of an AOI.
     * `aoilabeled`: Labeled boundary of an AOI.
    """

    url = "https://sdmdataaccess.nrcs.usda.gov/Spatial/SDMWM.wfs"
