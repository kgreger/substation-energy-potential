from download import load_or_fetch_mastr_data, load_or_fetch_osm_data
from export import save_results_to_geopackage, visualize_results
from map import (
    build_substation_summary,
    convert_to_geodataframes,
    create_connection_lines,
    match_nearest_substation,
)
from model import model_yield
from transform import transform_plants_data, transform_substation_data


def run_pipeline():
    # 0. Initialize constants and parameters
    BASE_URL = "https://www.marktstammdatenregister.de/MaStR/Einheit/EinheitJson/GetErweiterteOeffentlicheEinheitStromerzeugung?sort=&page=1&pageSize=25000&group=&filter=Energietr%C3%A4ger~eq~%272495%2C2497%27~and~Betriebs-Status~eq~%2735%27~and~Bundesland~eq~%271400%27&forExport=true"
    MASTR_PARQUET_PATH = "../data/mastr_brandenburg.parquet"
    OSM_PARQUET_PATH = "../data/osm_brandenburg.parquet"
    GPKG_PATH = "../output/solution.gpkg"
    HTML_PATH = "../output/interactive_map.html"
    YIELD_MWH_PER_MW = {
        "wind": 2200,
        "solar": 1000,
    }
    MIN_VOLTAGE_V = 20_000

    # load or fetch MaStR data for Brandenburg
    plants_raw = load_or_fetch_mastr_data(BASE_URL, MASTR_PARQUET_PATH)
    plants_transformed = transform_plants_data(plants_raw)
    plants_modeled = model_yield(plants_transformed, YIELD_MWH_PER_MW)

    # load or fetch OSM substation data for Brandenburg
    substations_raw = load_or_fetch_osm_data(OSM_PARQUET_PATH)
    substations_transformed = transform_substation_data(
        substations_raw, min_voltage_v=MIN_VOLTAGE_V
    )

    # map plants to substations using nearest-neighbor analysis and create connection lines
    gdf_plants, gdf_substations = convert_to_geodataframes(
        plants_modeled, substations_transformed
    )
    gdf_plants_matched = match_nearest_substation(gdf_plants, gdf_substations)
    gdf_connection_lines = create_connection_lines(gdf_plants_matched, gdf_substations)

    # aggregate yields per substation
    gdf_substations_summary = build_substation_summary(
        gdf_plants_matched, gdf_substations
    )

    # export results to GeoPackage and build interactive map
    save_results_to_geopackage(
        GPKG_PATH, gdf_plants_matched, gdf_substations_summary, gdf_connection_lines
    )
    visualize_results(
        HTML_PATH, gdf_plants_matched, gdf_substations_summary, gdf_connection_lines
    )


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
