# Substation Energy Potential in Brandenburg

## Table of Contents
- [Substation Energy Potential in Brandenburg](#substation-energy-potential-in-brandenburg)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Pipeline structure](#pipeline-structure)
    - [1. Data ingestion](#1-data-ingestion)
      - [MaStR plant data](#mastr-plant-data)
      - [OSM substation data](#osm-substation-data)
    - [2. Data transformation](#2-data-transformation)
      - [Plant filtering and standardization](#plant-filtering-and-standardization)
      - [Substation filtering](#substation-filtering)
    - [3. Energy modeling](#3-energy-modeling)
    - [4. Spatial matching](#4-spatial-matching)
      - [GeoDataFrame conversion](#geodataframe-conversion)
      - [Nearest-neighbor assignment](#nearest-neighbor-assignment)
      - [Connection lines](#connection-lines)
    - [5. Aggregation](#5-aggregation)
    - [6. Export](#6-export)
    - [7. Visualization](#7-visualization)
  - [Key design decisions](#key-design-decisions)
    - [Use of OSM for substations](#use-of-osm-for-substations)
    - [Nearest-neighbor matching](#nearest-neighbor-matching)
    - [Early filtering](#early-filtering)
    - [Use of GeoPandas](#use-of-geopandas)
    - [Use of Parquet](#use-of-parquet)
    - [Simple yield model](#simple-yield-model)
  - [Three analytical layers instead of one merged monster table](#three-analytical-layers-instead-of-one-merged-monster-table)
  - [Outputs](#outputs)
  - [How to run](#how-to-run)
    - [Install dependencies:](#install-dependencies)
    - [Run the pipeline:](#run-the-pipeline)
  - [Summary](#summary)


## Overview

This project builds an end-to-end ETL pipeline to estimate energy potential at substation level in Brandenburg, Germany.

The pipeline combines:
- plant-level data from the German MaStR registry
- substation data from OpenStreetMap
- spatial matching via nearest-neighbor analysis
- simple but realistic energy yield modeling

The final outputs include:
- enriched plant dataset with estimated annual energy and assigned substation
- substation-level aggregation of capacity and energy
- geospatial outputs for visualization
- an interactive map

Execution starts in `/src/main.py`, which orchestrates the full pipeline. 


## Pipeline structure

The pipeline follows a modular ETL structure: download → transform → model → map → export. Each step is implemented in a separate module.


### 1. Data ingestion

#### MaStR plant data

Plant data is downloaded from the [MaStR registry](https://www.marktstammdatenregister.de/MaStR/Einheit/Einheiten/ErweiterteOeffentlicheEinheitenuebersicht) using a paginated HTTP endpoint. Pagination is handled dynamically until no more records are returned. Results are normalized into a flat table. Data is cached locally as Parquet in `/data` and only refreshed once per day. This avoids unnecessary API calls and improves runtime efficiency and reproducibility.

[Open Power System Data](https://data.open-power-system-data.org/renewable_power_plants/2020-08-25) was also considered, but rejected due to outdated data.


#### OSM substation data

Substation data is retrieved from [OpenStreetMap](https://www.openstreetmap.org/#map=8/52.473/12.052) via the [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) using a custom query, `power=substation` objects are selected. Small distribution substations are excluded at query level. Nodes, ways, and relations are supported, ways and relations are converted to point geometries using their center. The data is also cached locally as Parquet in `/data`.

[ENTSO-E](https://www.entsoe.eu/data/map/) was also considered, but rejected due to the absence of an accessible API.


### 2. Data transformation

#### Plant filtering and standardization

The raw MaStR dataset is filtered early to keep only relevant assets:
- onshore wind plants
- utility-scale solar plants
- valid coordinates and capacity required

This reduces computational overhead and ensures data quality.

Key transformations:
- capacity converted from kW to MW
- coordinates standardized
- technology classification (wind vs solar)
- boolean flags for asset types

Filtering is done before further processing to avoid unnecessary computation on irrelevant data. 

#### Substation filtering

Substations are filtered based on voltage:
- voltage strings are parsed into numeric values
- the maximum voltage level is extracted
- only substations with voltage ≥ 20 kV are retained

This removes low-voltage distribution substations and focuses on grid-relevant infrastructure.


### 3. Energy modeling

The implementation maps technology (solar vs. wind) to yield factors and multiplies by installed capacity: `energy = capacity_mw * yield_factor`. This approach is transparent, easy to adjust, and sufficient for comparative analysis. 

Annual energy yield is estimated using simple technology-specific assumptions:
- wind: 2,200 MWh per MW
- solar: 1,000 MWh per MW

These values reflect typical full-load hours for Brandenburg, derived from installed capacity and generation data from an [analysis by Fraunhofer ISE](https://www.ise.fraunhofer.de/en/press-media/press-releases/2026/german-public-electricity-generation-in-2025-wind-and-solar-power-take-the-lead.html):
- onshore wind generation (2025): ~106 TWh
- installed onshore wind capacity: ~68 GW
- solar generation (2025): ~87 TWh
- installed solar capacity: ~116.8 GW

Using $CF = generation / (capacity * 8760)$ returns average capacity factors of ~18% for wind and ~8.5% for solar across Germany. 

Since Brandenburg is one of the strongest wind regions in Germany, has a very high installed capacity per capita and density plus flat terrain, as well as a high number of large utility-scape solar plants, generally higher capacity factors can be assumed:
- Onshore wind: 22-27%
- Solar: 10-12%

Actual plant-level yield (MWh/MW) cannot be computed directly from available open datasets, since MaStR has plant-level capacity and location, but no generation data, while OPSD has generation time series, but usually only aggregated by TSO or on country level. Hence, the values used here produce realistic order-of-magnitude estimates but do not represent plant-specific metered output.

Brandenburg has around 9 GW of installed wind capacity. Assuming typical German onshore wind full-load hours of 2,000–2,500 hours per year, this corresponds to approximately 18–22 TWh of annual wind generation ([Source](https://www.businesslocationcenter.de/en/energytechnologies/renewable-energy)). The current modeling choice of 2,200 MWh/MW sits right in the middle of that range and is credible, but can be changed easily in the code.

Unlike wind, solar output is much less variable spatially. Yields are driven mainly by irradiation and orientation. Brandenburg is not dramatically different from the German average, therefore using a conservative yield assumption (1,000 MWh/MW) is not only acceptable but standard practice for this type of analysis. It, too, can be changed easily in the code.


### 4. Spatial matching

#### GeoDataFrame conversion

Plant and substation data are converted into [GeoDataFrames](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html). Using a projected CRS enables accurate distance calculations in meters:
- initial CRS: EPSG:4326 (WGS84)
- projected CRS: EPSG:25833 (UTM zone 33N)

#### Nearest-neighbor assignment

Each plant is assigned to the closest substation using [`geopandas.sjoin_nearest()`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sjoin_nearest.html). This provides:
- assigned substation ID
- distance in meters and kilometers
- matching method metadata (added for reproducibility and future comparison purposes)

The approach is simple and robust, given that no official plant-to-substation mapping exists.

#### Connection lines

For visualization and validation, LineString geometries are created between plant locations and assigned substations. Intermediate geometries are removed to ensure clean geospatial outputs. 


### 5. Aggregation

Substation-level metrics are computed by grouping plants by assigned substation. Metrics include:
- Totals
    - total capacity (MW)
    - total estimated annual energy (MWh)
- Technology breakdown
    - wind capacity (MW) and energy (MWh)
    - solar capacity (MW) and energy (MWh)
    - plant counts by technology
- Quality indicators
    - average distance to substation (km)
    - maximum distance to substation (km)

Totals are computed directly from base columns rather than derived from subtotals to ensure robustness. The aggregated dataset is then merged back with substation metadata and geometry. 


### 6. Export

Results are exported to `/output` as a [GeoPackage](https://www.geopackage.org/) with multiple layers:
- plants (point layer)
- substations (point layer)
- plant-substation connections (line layer)

GeoPackage is chosen because it is widely supported by GIS tools such as QGIS, supports multiple layers in a single file, and preserves geometry and CRS information.


### 7. Visualization

An interactive map is created using [Folium](https://github.com/python-visualization/folium). Plants are styled by technology (wind: blue circles, solar: orange circles), substations are shown as black squares. Connection lines are drawn between plants and substations.

Additional features:
- multiple basemaps (OpenStreetMap, light base map, satellite imagery)
- popups with plant and substation details
- layer control for toggling visibility
- custom legend

The map is exported as a self-contained HTML file to `/output` for easy sharing. 


## Key design decisions

### Use of OSM for substations

No official, complete substation dataset is freely available. OpenStreetMap provides:
- good spatial coverage
- sufficient metadata
- up-to-date community-driven data

Filtering by voltage ensures relevance for grid-level analysis.


### Nearest-neighbor matching

There is no public mapping between plants and substations. Assigning plants to the nearest substation is simple and explainable, produces reasonable approximations, and allows spatial validation via visualization. Distance metrics are included to identify potential mismatches.


### Early filtering

Filtering plant data before transformation reduces memory usage, improves performance and avoids unnecessary computation.


### Use of GeoPandas

GeoPandas is used for coordinate handling, CRS transformations, spatial joins, and geometry creation. This simplifies spatial logic and keeps the pipeline readable.


### Use of Parquet

Intermediate datasets are stored as Parquet in `/data` which ensures efficient storage, fast read/write, and is suitable for columnar processing. Daily caching avoids repeated downloads.


### Simple yield model

The yield model is intentionally simple:
- transparent assumptions
- easy to modify
- sufficient for relative comparison across substations

What matters is that these are modeled averages, not plant-specific predictions. A real plant can differ because of:
- commissioning year and curtailment
- turbine model, hub height, rotor diameter etc.
- exact solar orientation and shading
- local wind and irradiation conditions

More complex modeling would therefore require weather and plant-specific data, which is outside the scope.


## Three analytical layers instead of one merged monster table

- plants: one row per plant
- substations: one row per substation
- connections: one row per plant-to-substation match

This normalized table structrure provides for clean joins, easy aggregation, and straightforward map rendering.

For numeric analysis it is critical to have one clear fact table for assignments. This avoids duplicated substation attributes repeated across every plant row and allows for easy grouping by substation, technology, and potentially other attributes.

For geographic visualization stable point geometries for both plants and substations are required, as well as a separate table for the connecting lines, to be able to draw plant→substation links.


## Outputs

The pipeline produces to files in `/data`:
- `solution.gpkg` with three layers: plants, substations, and connection lines
- `interactive_map.html`

These outputs support analytical workflows, GIS-based exploration, and visual validation.


## How to run


### Install dependencies:

`pip install pandas geopandas folium shapely requests` or `uv add pandas geopandas folium shapely requests`.


### Run the pipeline:

`python main.py` or `uv run main.py`.


## Summary

This project demonstrates how to combine open datasets, spatial analysis, and simple modeling to approximate substation-level energy potential. The result is a flexible dataset that supports both quantitative analysis of grid utilization and intuitive geographic exploration. The approach prioritizes clarity, robustness, and reproducibility over unnecessary complexity.