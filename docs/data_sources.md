# Landmapper Data Pipeline (lm-dpl) Data Sources

## Parcel Data 

***ODF Forest Protection Districts (fpd)*** 

**Description**: This dataset shows the boundaries of Oregon's Forest Protection Districts, managed by the Oregon Department of Forestry (ODF). It shows which geographic zones fall under specific fire prevention and suppression responsibility areas. Communities can use it to see which district oversees forestry protection for their land.   
**Agency**: Oregon Department of Forestry (ODF)  
**Contact**: Steve Timbrook, [steve.timbrook@odf.oregon.gov](mailto:steve.timbrook@odf.oregon.gov)  
**Published**: N/A  
**Date Accessed**:   
**URL**: [Layer: ProtectionPDFs (ID:4)](https://gis.odf.oregon.gov/ags1/rest/services/Applications/ProtectionPDFs/MapServer/4)   
**Fields**:   
  - `ODF_FPD`: Forest Protection Districts Description

***Oregon Zoning Map (zoning)*** 

**Description**: Statewide zoning map compiled by DLCD and ODOT, which combines zoning data from 229 cities and 30 counties (last updated 06/2023); shows how land is zoned across the state, with some areas still marked "not shared".  
**Agency**: Department of Land Conservation and Development (DLCD)  
**Published**: 2023  
**Updated**: July 19, 2023  
**Date Accessed**:   
**URL**: [Layer: ZoneOR_Gov2Pub (ID:0)](https://services8.arcgis.com/8PAo5HGmvRMlF2eU/ArcGIS/rest/services/Zoning/FeatureServer/0)   
**Fields**:   
  - `orZCode`: Zoning District Code  
  - `orZDesc`: Zoning District Description

***BLM PLSS First Division (plss1)***  

**Description**: This dataset maps the first-division boundaries of the Public Land Survey System across Oregon and Washington, which is maintained by the Bureau of Land Management Oregon State Office. It provides standardized cadastral boundaries used for land ownership, legal descriptions, and resource management in alignment with FGDC Cadastral Data Content Standards.   
**Agency**: Bureau of Land Management (BLM), Oregon State Office.  
**Published**: NA  
**Updated**: NA  
**Accessed**:   
**URL**: [Layer: First Division (ID:4)](https://gis.blm.gov/orarcgis/rest/services/Land_Status/BLM_OR_PLSS/MapServer/4)  
**Fields**:  
  - `PLSSID`: PLSS Identifier  
  - `FRSTDIVNO`: PLSS First Division Number  
  - `FRSTDIVTXT`: PLSS First Division Type Text

***BLM PLSS Township and Range (plss2)*** 

**Description**: This dataset outlines the township and range grid of the Public Land Survey System (PLSS) across Oregon and Washington. The Bureau of Land Management, Oregon State Office, maintains it. It provides standardized cadastral boundaries for identifying and mapping legal land descriptions, supporting land management, ownership records, and resource planning.   
**Agency**: Bureau of Land Management (BLM), Oregon State Office.  
**Published**: NA  
**Updated**: NA  
**Accessed**:   
**URL**: [Layer: Township and Range (ID:2)](https://gis.blm.gov/orarcgis/rest/services/Land_Status/BLM_OR_PLSS/MapServer/2)  
**Fields**:   
  - `PLSSID`: Public Land Survey System Identifier  
  - `TWNSHPLAB`: PLSS Township Label

***Oregon Structural Fire Districts (sdf)*** 

**Description**: This dataset maps the boundaries and service areas of Oregon's structural fire protection districts, including agency names, coverage status, and population data. Maintained by the Oregon State Fire Marshal and partner agencies, it supports statewide emergency response planning, mutual aid coordination, and community risk assessment.   
**Published**: August 20, 2024  
**URL**: [Layer: StructuralFireDistricts (ID:68)](https://services.arcgis.com/uUvqNMGPm7axC2dD/ArcGIS/rest/services/Structural_Fire_Districts_Public/FeatureServer/68)  
**Fields**:  
  - `FDID_DCIN`: Fire Department Identifier  
  - `Agency_Name`: Name of fire agency

***Oregon Tax Lots (taxlots)***

**Description**: County tax assessors tax lots with associated property data. The tax lot's spatial features and related data are compiled by the Oregon Geospatial Enterprise Operations (GEO) from existing records created and maintained by local county assessment and taxation offices.   
**Agency**: Oregon Department of Revenue  
**Contact**: Philip McClellan (Phone: (503) 586-8128) and Thomas York (Phone: (503) 302-5078).   
**Published**: NA  
**URL**: [Layer: DOR_ORMAP (ID:3)](https://utility.arcgis.com/usrsvcs/servers/78bbb0d0d9c64583ad5371729c496dcc/rest/services/Secure/DOR_ORMAP/MapServer/3/query)  
**Fields**:  
  - `MAPTAXLOT`: Map and taxlot number. This field connects the property table to the taxlot geometry. 

***ODFW Conservation Opportunity Areas (coa)***

**Description**: This dataset delineates Oregon's Conservation Opportunity Areas (COAs), which identify priority places for voluntary fish and wildlife habitat conservation. Developed by the Oregon Department of Fish and Wildlife for the Oregon Conservation Strategy, it supports habitat restoration, land use planning, and coordinated conservation efforts across ownership boundaries.   
**Agency**: Oregon Department of Fish and Wildlife  
**Published**: December 29, 2023  
**Contact**: Oregon Conservation Strategy (email: [Conservation.Strategy@odfw.oregon.gov](mailto:Conservation.Strategy@odfw.oregon.gov))  
**URL**: [Layer: ODFW_ConcervationOpportunityAreas (ID:0)](https://nrimp.dfw.state.or.us/arcgis/rest/services/Compass/ODFW_ConservationOpportunityAreas/MapServer/0/query)  
**Fields**:  
  - `COAID`: COA Unique Identifier  
  - `COAName`: COA Designated Name

***Washington Taxlots (taxlots)***

**Description**: This dataset, part of the Washington State Parcels Project, offers a comprehensive collection of tax parcel information for counties with digital records. Compiled by Washington Technology Solutions (WaTech) in collaboration with county assessors, it standardizes parcel attributes across all participating counties, including parcel IDs, situs addresses, land use codes, property values, and assessor website links.   
**Agency**: Washington Technology Solutions (WaTech) & Washington State Counties  
**Published**: 2024  
**Last Updated**: April 28, 2025
**URL**: [Layer: Previous_Parcels (ID:0)](https://services.arcgis.com/jsIt88o09Q0r1j8h/ArcGIS/rest/services/Previous_Parcels/FeatureServer/0/query)  
**Fields**:  
  - `FIPS_NR`: Federal Information Processing Standards Code  
  - `LANDUSE_CD`: Land use code as determined by the Department of Revenue.  
  - `COUNTY_NM`: County Name where parcels are located.  
  - `FILE_DATE`: Date associated with the parcel record

***Washington State Zoning Atlas (zoning)*** 

**Description**: The Washington State Zoning Atlas (WAZA) is the first statewide tool designed to standardize and present zoning data across Washington state, translating local zoning designations into consistent land use categories and attributes. It provides a spatial framework for understanding zoning patterns, land use overlays, and local government jurisdictions.  
**Agency**: Washington State Office of Financial Management (OFM)   
**Published**: 2024  
**URL**: [Layer: WAZA_Prototype_Layers (ID:0)](https://services6.arcgis.com/tboeqGwETr5ppr5Q/ArcGIS/rest/services/WAZA_Prototype_Layers/FeatureServer/0/query)  
**Fields**:   
  - `ZoneID`: Standardized zoning code  
  - `ZoneName`: Standardized zoning name

***Washington Public Land Survey System (plss)***

**Description**: This dataset contains the legal land description framework for Washington State. It encompasses Public Land Survey System (PLSS) townships, sections, donation land claims, tracts, and other subdivisions used to define parcel boundaries. It provides the foundational spatial framework supporting land ownership records, property taxation, and cadastral mapping across the state.   
**Agency**: Washington State Department of Natural Resources  
**Published**: 2024  
**URL**: [Query: WA Legal Descriptions (ID:2)](https://gis.dnr.wa.gov/site3/rest/services/Public_Boundaries/WADNR_PUBLIC_Cadastre_OpenData/MapServer/2/query)  
**Fields**:   
  - `LEGAL_DESC_TYPE_CD`: Legal description code  
  - `LEGAL_DESC_NM`: Legal description identifying the parcel boundaries

***Washington Fire Protection Districts (fpd)***

**Description**: This dataset delineates fire protection districts and municipal fire departments in Washington State that maintain agreements with the Department of Natural Resources (DNR). It is used to document and display the types of fire protection agreements between DNR and local jurisdictions.   
**Agency**: Washington State Department of Natural Resources (DNR) - Wildland Fire Management Division.  
**Published**: Not listed in the service metadata.   
**URL**: [Layer: WADNR_PUBLIC_WD_WildFire_Data (ID:6)](https://gis.dnr.wa.gov/site3/rest/services/Public_Wildfire/WADNR_PUBLIC_WD_WildFire_Data/MapServer/6/query)  
**Fields**:   
  - `FPD_CODE`: Fire Protection District unique identifier  
  - `FPD_DESC`: Fire Protection District label

***12-digit Hydrologic Unit (huc)***

**Description**: This dataset depicts 12-digit Hydrologic Unit boundaries (subwatersheds) across the U.S., Puerto Rico, and the U.S. Virgin Islands. It is derived from the USGS National Hydrography Dataset and the USDA NRCS Watershed Boundary Dataset. It supports hydrologic analysis, watershed management, and regional to national scale mapping.   
**Agency**: U.S. Geological Survey (USGS)  
**Published**: Not listed in the service metadata.   
**URL**: [Query: 12-digit HU (Subwatershed) (ID:6)](https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/6/query)  
**Fields**:   
  - `HUC12`: 12-digit Hydrologic Unit Code  
  - `NAME`: Subwatershed unit name  
  - `STATES`: U.S. outlying areas in which the hydrologic unit is located

***USA Census Populated Place Areas (ppa)***

**Description**: This dataset represents U.S. Census populated place areas from the 2020 Census. It includes incorporated cities and census-designated places. It provides population and area data for each place, supporting demographic analysis, cartographic display, and local-to-national planning applications.   
**Agency**: U.S. Department of Commerce, U.S. Census Bureau   
**Published**: November 21, 2024  
**URL**: [Layer: USA_Census_Populated_Places (ID:0)](https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Census_Populated_Places/FeatureServer/0/query)  
**Fields**:  
  - `FID`: Unique identifier  
  - `CLASS`: Categorizes the type of populated place (e.g., incorporated place, city, or census-designated place)  
  - `ST`: Two-letter postal abbreviation for the state  
  - `STFIPS`: Two-digit FIPS code for the state  
  - `PLACEFIPS`: FIPS code for the populated place  
  - `POPULATION`: Total population count from the last census  
  - `POP_CLASS`: Classification based on population range  
  - `POP_SQMI`: Population density  
  - `SQMI`: Area in square miles

## Soil Data

***USDA NRCS Soil Survey (soil)***

**Description**: This dataset contains spatial and tabular soil data from the Soil Survey Geographic (SSURGO) database, retrieved via the USDA NRCS Soil Data Access (SDA) service. It provides detailed soil map unit boundaries and attributes, including drainage class, potential erosion hazards, and forest productivity site indices, used for land management and conservation planning.  
**Agency**: USDA Natural Resources Conservation Service (NRCS)  
**Contact**: [Soil Data Access Support](https://sdmdataaccess.nrcs.usda.gov/)   
**Published**: Varies by survey area (typically updated annually)   
**Date Accessed**: Dynamic (fetched via API)  
**URL**: [SDA API](https://sdmdataaccess.nrcs.usda.gov/tabular/post.rest)   
**Table:Fields**:
  - `muaggatt: mukey`: Map Unit Key
  - `muaggatt: muname`: Map Unit Name
  - `muaggatt: drclassdcd`: Drainage Class (Dominant Condition)
  - `muaggatt: forpehrtdcp`: Potential Erosion Hazard (Road/Trail)
  - `component: comppct_r`: Component Percentage (Representative Value)
  - `coforprod: plantcomname`: Plant Community Name
  - `coforprod: siteindex_r`: Site Index (Representative Value)
  - `corestrictions: resdept_l`: Restriction Depth (Low Value)
  - `corestrictions: resdept_h`: Restriction Depth (High Value)

## Remote Sensing

***Elevation***

**Description**: 3DEP Digital Elevation Model (DEM) for the U.S. with full coverage of the 48 conterminous states, Hawaii, and U.S. territories. Ground spacing is approximately 10 meters north/south. The dataset is retrieved via Google Earth Engine and used to calculate minimum and maximum elevation statistics for parcels.  
**Agency**: United States Geological Survey (USGS)  
**Dataset Availability**: 1998-08-16 to 2020-05-06  
**URL**: [USGS 3DEP 10m Collection](https://developers.google.com/earth-engine/datasets/catalog/USGS_3DEP_10m_collection)   
**Bands**:
  - `elevation`: Elevation above sea level in meters

***Forest Cover***

**Description**: Dynamic World is a near real-time 10m resolution global land use/land cover dataset produced using deep learning and Sentinel-2 imagery. It provides class probabilities and label information across nine land cover classes, including trees, which is used to calculate forest coverage statistics for parcels.
**Agency**: Google & World Resources Institute  
**Dataset Availability**: 2015-06-27 to present (updated daily)   
**URL**: [Google Dynamic World V1](https://developers.google.com/earth-engine/datasets/catalog/GOOGLE_DYNAMICWORLD_V1)  
**Bands**:
  - `label`: Discrete land cover classification with values 0-8 (0=water, 1=trees, 2=grass, 3=flooded_vegetation, 4=crops, 5=shrub_and_scrub, 6=built, 7=bare, 8=snow_and_ice)
