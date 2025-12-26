## Complete Pipeline Execution

### Running the full pipeline for Washington State.

#### Fetching data required for taxlot and populationpoint tables:

Check endpoint connectivity
```bash
lm-dpl test-endpoints --state washington
```

Fetch all layers for Washington (~2 hrs including postprocessing steps): 
1. By default, this command fetches all layers with `fetch` option enabled in [`lm_dpl/clients/endpoints.yaml`](../lm_dpl/clients/endpoints.yaml) configuration.
2. Soil and elevation layers won't be fetched with this command, must be fetched separately.

```bash
lm-dpl fetch washington
```

Alternatively, you can fetch layers one by one if you need more control over what data to fetch.
```bash
lm-dpl fetch --layer taxlots washington 
lm-dpl fetch --layer zoning washington
lm-dpl fetch --layer landuse washington
lm-dpl fetch --layer plss washington
lm-dpl fetch --layer fpd washington
lm-dpl fetch --layer huc washington
lm-dpl fetch --layer ppa washington
lm-dpl fetch --layer cty washington
```

#### Fetch soil and elevation data:

Fetch soil data only (~10 min)
```bash
lm-dpl fetch --layer soil washington
```

fetch elevation data (~5 hrs for entire state if elevation not previously fetched): 
- Make sure raster data has been downloaded to `.data/gee3dep` and `.data/geedynamicworld` directories. If not, run [notebooks/fetch_dem_dw_rasters.ipynb](../notebooks/fetch_dem_dw_rasters.ipynb). 
- Fetch elevation only after fetching taxlots.

```bash
lm-dpl fetch --layer elevation washington 
```

#### Processing final app tables

This step assumes that all necessary data has been fetched and is available in staging tables.

```bashExport to a compressed SQL dump
lm-dpl process --table taxlots --state washington
lm-dpl process --table soil --state washington
lm-dpl process --table populationpoint --state washington
```

#### Export app tables 

Export to a compressed SQL dump
```bash
docker exec -i dpl-db pg_dump -U gis -d gis -t public.washington_app_taxlot | gzip -c > washington_app_taxlot.sql.gz
docker exec -i dpl-db pg_dump -U gis -d gis -t public.washington_app_soiltype | gzip -c > washington_app_soiltype.sql.gz
docker exec -i dpl-db pg_dump -U gis -d gis -t public.washington_app_populationpoint | gzip -c > washington_app_populationpoint.sql.gz
```

### Running the full pipeline for Oregon State.

#### Fetching data required for taxlot, coa, and populationpoint tables:

Check endpoint connectivity
```bash
lm-dpl test-endpoints --state oregon
```

Fetch all layers for Oregon (~2 hrs including postprocessing steps)
NOTES: 
1. By default, this command fetches all layers with `fetch` option enabled in [`lm_dpl/clients/endpoints.yaml`](../lm_dpl/clients/endpoints.yaml) configuration.
2. Soil and elevation layers won't be fetched with this command, must be fetched separately.
3. Oregon taxlots endpoint is currently set to `fetch: false`. Instructions to fetch taxlot data are provided below.

```bash
lm-dpl fetch oregon
```

Alternatively, you can fetch layers one by one if you need more control over what data to fetch.
```bash
lm-dpl fetch --layer fpd oregon
lm-dpl fetch --layer zoning oregon
lm-dpl fetch --layer plss1 oregon
lm-dpl fetch --layer plss2 oregon # fetch plss2 after plss1 
lm-dpl fetch --layer sfd oregon
lm-dpl fetch --layer coa oregon
lm-dpl fetch --layer huc oregon
lm-dpl fetch --layer ppa oregon
lm-dpl fetch --layer cty oregon
```

#### Fetch Oregon taxlot data from county endpoints and shapefiles:

Test county endpoints connectivity.
Make sure option `fetch` is set to true in the configuration file.
```bash
 lm-dpl test-endpoint --config lm_dpl/config/county_taxlots_endpoints.yaml 
```

Import taxlot data from Oregon counties endpoints
```bash
 lm-dpl fetch --config lm_dpl/config/county_taxlots_endpoints.yaml
```

Import taxlot data provided as shapefiles. These files are stored in the FESData external data repository.
Use pattern "s_oregon_taxlots_<county name>" for table names.

```bash
 DATADIR=/mnt/data/FESDataRepo/external/taxlots
 lm-dpl import-file $DATADIR/TaxlotsPolk/txltprp27.shp s_oregon_taxlots_polk --t-srid 3857
 lm-dpl import-file $DATADIR/TaxlotsClackamas/tax_parcels_fixed.shp s_oregon_taxlots_clackamas --srid 2913 --t-srid 3857
 lm-dpl import-file $DATADIR/TaxlotsCrook/Taxlots.shp s_oregon_taxlots_crook --t-srid 3857
 lm-dpl import-file $DATADIR/TaxlotsLincoln/taxlot21.shp s_oregon_taxlots_lincoln --t-srid 3857
 ```

 Run script to consolidate Oregon taxlot data (creates s_oregon_taxlots table)
 
 This script will also import the remaining taxlot data for Linn, Malheur, Jefferson, Gilliam, Sherman, and Grant from last data refresh. You may need to update the code as new county data is added.

 ```bash
 cat lm_dpl/parcels/oregon_taxlot_from_county.sql | sudo docker exec -i dpl-db psql -U gis -d gis
```

Run post script.

```bash
 cat lm_dpl/parcels/oregon_taxlot_post.sql | sudo docker exec -i dpl-db psql -U gis -d gis
 ```

#### Fetch soil and elevation data:

Fetch soil data (~10 min)
```bash
lm-dpl fetch --layer soil oregon
```

Fetch elevation data (~4 hrs for entire state if elevation not previously fetched).
- Make sure raster data has been downloaded to `.data/gee3dep` and `.data/geedynamicworld` directories. If not, run [notebooks/fetch_dem_dw_rasters.ipynb](notebooks/fetch_dem_dw_rasters.ipynb). 
- Fetch elevation data only after fetching taxlots. 

```bash
lm-dpl fetch --layer elevation oregon 
```

#### Processing final app tables

This step assumes that all necessary data has been fetched and is available in staging tables.


```bash
lm-dpl process --table taxlots --state oregon
lm-dpl process --table soil --state oregon
lm-dpl process --table populationpoint --state oregon
lm-dpl process --table coa --state oregon
```

#### Export app tables 

```bash
# Export Oregon app_taxlot table to a compressed SQL dump
docker exec -i dpl-db pg_dump -U gis -d gis -t public.oregon_app_taxlot | gzip -c > oregon_app_taxlot.sql.gz
docker exec -i dpl-db pg_dump -U gis -d gis -t public.oregon_app_soiltype | gzip -c > oregon_app_soiltype.sql.gz
docker exec -i dpl-db pg_dump -U gis -d gis -t public.oregon_app_populationpoint | gzip -c > oregon_app_populationpoint.sql.gz
docker exec -i dpl-db pg_dump -U gis -d gis -t public.oregon_app_coa | gzip -c > oregon_app_coa.sql.gz
```

## Schema changes

### Soil Data

Changes in table schemas must be reflected in the following files:
- `lm_dpl/soil/processor.py`: 
    - Update SQL queries to include new/existing columns
    - Update data insertion logic to match new schema
- `lm_dpl/soil/oregon_soils_schema.sql`
    - Update `CREATE TABLE` statements to reflect new/existing columns
    - Ensure data types and constraints are correctly defined

### Updating Taxlot Data

Changes in table schemas must be reflected in the following files:
- `lm_dpl/clients/endpoints.py`:
    - Add new data source endpoints if applicable
    - Change source column names (outfields) and data types (dtypes)
    - Add `post_script` to handle postprocessing steps, e.g. renaming columns or adding new ones.
- `lm_dpl/parcels/**.sql`
    - Update scripts to reflect new/existing data layers and schema changes
    - Ensure data types and constraints are correctly defined
