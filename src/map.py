import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString


def convert_to_geodataframes(
    df_plants: pd.DataFrame,
    df_substations: pd.DataFrame,
    target_crs: str = "EPSG:25833",
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Convert plants and substations DataFrames to GeoDataFrames with appropriate geometry and CRS.

    Args:
        df_plants (pd.DataFrame): DataFrame containing modeled plant data with 'lon' and 'lat'
        df_substations (pd.DataFrame): DataFrame containing substation data with 'lon' and 'lat'
    Returns:
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: A tuple containing the plants and substations GeoDataFrames in the target CRS
    """

    gdf_plants = gpd.GeoDataFrame(
        df_plants.copy(),
        geometry=gpd.points_from_xy(df_plants["lon"], df_plants["lat"]),
        crs="EPSG:4326",
    )

    gdf_substations = gpd.GeoDataFrame(
        df_substations.copy(),
        geometry=gpd.points_from_xy(df_substations["lon"], df_substations["lat"]),
        crs="EPSG:4326",
    )

    gdf_plants_proj = gdf_plants.to_crs(target_crs)
    gdf_substations_proj = gdf_substations.to_crs(target_crs)

    return gdf_plants_proj, gdf_substations_proj


def match_nearest_substation(
    gdf_plants: gpd.GeoDataFrame, gdf_substations: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Perform nearest-neighbor spatial join to match each plant to the closest substation.

    Args:
        gdf_plants (gpd.GeoDataFrame): GeoDataFrame containing plant data
        gdf_substations (gpd.GeoDataFrame): GeoDataFrame containing substation data

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing matched plant and substation data
    """

    gdf_plants_matched = gpd.sjoin_nearest(
        gdf_plants[
            [
                "plant_id",
                "plant_name",
                "technology",
                "capacity_mw",
                "estimated_annual_energy_mwh",
                "geometry",
            ]
        ],
        gdf_substations[
            ["substation_id", "substation_name", "max_voltage_v", "geometry"]
        ],
        how="left",
        distance_col="distance_to_substation_m",
    )

    gdf_plants_matched["distance_to_substation_km"] = (
        gdf_plants_matched["distance_to_substation_m"] / 1000
    )
    gdf_plants_matched["matching_method"] = "gpd_sjoin_nearest_epsg25833"

    gdf_plants_matched = gdf_plants_matched.rename(
        columns={
            "substation_id": "assigned_substation_id",
            "substation_name": "assigned_substation_name",
            "max_voltage_v": "assigned_substation_max_voltage_v",
        }
    )

    return gdf_plants_matched


def create_connection_lines(
    gdf_plants: gpd.GeoDataFrame, gdf_substations: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Create LineString geometries representing connections between plants and their assigned substations.

    Args:
        gdf_plants (gpd.GeoDataFrame): GeoDataFrame containing plant data with assigned substation information
        gdf_substations (gpd.GeoDataFrame): GeoDataFrame containing substation data
    Returns:
        gpd.GeoDataFrame: GeoDataFrame with LineString geometries for plant-substation connections
    """

    gdf_plants_geom = gdf_plants[["plant_id", "plant_name", "geometry"]].rename(
        columns={"geometry": "plant_geometry"}
    )

    gdf_substations_geom = gdf_substations[
        ["substation_id", "substation_name", "geometry"]
    ].rename(columns={"geometry": "substation_geometry"})

    gdf_lines = gdf_plants[["plant_id", "technology", "assigned_substation_id"]].copy()
    gdf_lines = gdf_lines.merge(gdf_plants_geom, on="plant_id", how="left")
    gdf_lines = gdf_lines.merge(
        gdf_substations_geom,
        left_on="assigned_substation_id",
        right_on="substation_id",
        how="left",
    )
    gdf_lines["geometry"] = gdf_lines.apply(
        lambda row: LineString([row["plant_geometry"], row["substation_geometry"]]),
        axis=1,
    )
    gdf_lines = gpd.GeoDataFrame(gdf_lines, geometry="geometry", crs=gdf_plants.crs)
    gdf_lines = gdf_lines.to_crs("EPSG:4326")
    gdf_lines = gdf_lines.drop(
        columns=["plant_geometry", "substation_geometry"],
        errors="ignore",
    )

    print(f"Established connections: {len(gdf_lines):,.0f}")

    return gdf_lines


def build_substation_summary(
    gdf_plants_matched: gpd.GeoDataFrame, gdf_substations: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Build a summary DataFrame aggregating total estimated annual energy yield per substation.

    Args:
        gdf_plants_matched (gpd.GeoDataFrame): GeoDataFrame containing plant data with assigned substation information and estimated annual energy yield
        gdf_substations (gpd.GeoDataFrame): GeoDataFrame containing substation data

    Returns:
        gpd.GeoDataFrame: Summary GeoDataFrame with total estimated annual energy yield per substation
    """

    gdf_plants_matched["wind_capacity_mw"] = gdf_plants_matched["capacity_mw"].where(
        gdf_plants_matched["technology"].eq("wind"),
        0.0,
    )

    gdf_plants_matched["solar_capacity_mw"] = gdf_plants_matched["capacity_mw"].where(
        gdf_plants_matched["technology"].eq("solar"),
        0.0,
    )

    gdf_plants_matched["wind_estimated_annual_energy_mwh"] = gdf_plants_matched[
        "estimated_annual_energy_mwh"
    ].where(
        gdf_plants_matched["technology"].eq("wind"),
        0.0,
    )

    gdf_plants_matched["solar_estimated_annual_energy_mwh"] = gdf_plants_matched[
        "estimated_annual_energy_mwh"
    ].where(
        gdf_plants_matched["technology"].eq("solar"),
        0.0,
    )

    gdf_plants_matched["is_wind"] = (
        gdf_plants_matched["technology"].eq("wind").astype(int)
    )
    gdf_plants_matched["is_solar"] = (
        gdf_plants_matched["technology"].eq("solar").astype(int)
    )

    df_substation_summary = (
        gdf_plants_matched.groupby("assigned_substation_id")
        .agg(
            plant_count=("plant_id", "count"),
            wind_plant_count=("is_wind", "sum"),
            solar_plant_count=("is_solar", "sum"),
            total_capacity_mw=("capacity_mw", "sum"),
            wind_capacity_mw=("wind_capacity_mw", "sum"),
            solar_capacity_mw=("solar_capacity_mw", "sum"),
            total_estimated_annual_energy_mwh=("estimated_annual_energy_mwh", "sum"),
            wind_estimated_annual_energy_mwh=(
                "wind_estimated_annual_energy_mwh",
                "sum",
            ),
            solar_estimated_annual_energy_mwh=(
                "solar_estimated_annual_energy_mwh",
                "sum",
            ),
            avg_distance_to_substation_km=("distance_to_substation_km", "mean"),
            max_distance_to_substation_km=("distance_to_substation_km", "max"),
        )
        .reset_index()
    )

    gdf_substation_summary = df_substation_summary.merge(
        gdf_substations[
            [
                "substation_id",
                "substation_name",
                "max_voltage_v",
                "geometry",
            ]
        ],
        left_on="assigned_substation_id",
        right_on="substation_id",
        how="right",
    )

    gdf_substation_summary = gpd.GeoDataFrame(
        gdf_substation_summary,
        geometry="geometry",
        crs=gdf_substations.crs,
    )

    return gdf_substation_summary
