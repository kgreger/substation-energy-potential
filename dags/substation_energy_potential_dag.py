import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
WORK_DIR = DATA_DIR / "airflow_temp"

YIELD_MWH_PER_MW = {
    "wind": 2200,
    "solar": 1000,
}
MIN_VOLTAGE_V = 20_000


with DAG(
    dag_id="substation_energy_potential_brandenburg",
    description="Estimate renewable energy potential by substation in Brandenburg.",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    start_date=datetime(2026, 4, 29),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["etl", "geospatial", "brandenburg", "solar", "wind", "mastr", "osm"],
) as dag:

    @task
    def prepare_directories() -> dict[str, str]:
        for path in [DATA_DIR, OUTPUT_DIR, WORK_DIR]:
            path.mkdir(parents=True, exist_ok=True)

        return {
            "data_dir": str(DATA_DIR),
            "output_dir": str(OUTPUT_DIR),
            "work_dir": str(WORK_DIR),
        }

    @task
    def ingest_mastr_plants() -> str:
        from src.download import load_or_fetch_mastr_data

        raw_cache = DATA_DIR / "mastr_brandenburg.parquet"
        out = WORK_DIR / "01_mastr_raw.parquet"

        df = load_or_fetch_mastr_data(raw_cache)
        df.to_parquet(out, index=False)

        return str(out)

    @task
    def transform_plants(mastr_raw_path: str) -> str:
        import pandas as pd

        from src.transform import transform_plants_data

        df = pd.read_parquet(mastr_raw_path)
        df = transform_plants_data(df)

        out = WORK_DIR / "02_plants_transformed.parquet"
        df.to_parquet(out, index=False)

        return str(out)

    @task
    def model_energy(plants_path: str) -> str:
        import pandas as pd

        from src.model import model_yield

        df = pd.read_parquet(plants_path)
        df = model_yield(df, YIELD_MWH_PER_MW)

        out = WORK_DIR / "03_plants_modeled.parquet"
        df.to_parquet(out, index=False)

        return str(out)

    @task
    def ingest_osm_substations() -> str:
        from src.download import load_or_fetch_osm_data

        raw_cache = DATA_DIR / "osm_brandenburg.parquet"
        out = WORK_DIR / "04_osm_substations_raw.parquet"

        df = load_or_fetch_osm_data(raw_cache)
        df.to_parquet(out, index=False)

        return str(out)

    @task
    def transform_substations(osm_raw_path: str) -> str:
        import pandas as pd

        from src.transform import transform_substation_data

        df = pd.read_parquet(osm_raw_path)
        df = transform_substation_data(df, min_voltage_v=20_000)

        out = WORK_DIR / "05_substations_transformed.parquet"
        df.to_parquet(out, index=False)

        return str(out)

    @task
    def convert_to_geo(plants_path: str, substations_path: str) -> dict[str, str]:
        import pandas as pd

        from src.map import convert_to_geodataframes

        plants = pd.read_parquet(plants_path)
        substations = pd.read_parquet(substations_path)

        gdf_plants, gdf_substations = convert_to_geodataframes(plants, substations)

        plants_out = WORK_DIR / "06_plants.geo.parquet"
        substations_out = WORK_DIR / "07_substations.geo.parquet"

        gdf_plants.to_parquet(plants_out, index=False)
        gdf_substations.to_parquet(substations_out, index=False)

        return {
            "plants": str(plants_out),
            "substations": str(substations_out),
        }

    @task
    def match_spatially(paths: dict[str, str]) -> str:
        import geopandas as gpd

        from src.map import match_nearest_substation

        gdf_plants = gpd.read_parquet(paths["plants"])
        gdf_substations = gpd.read_parquet(paths["substations"])

        matched = match_nearest_substation(gdf_plants, gdf_substations)

        out = WORK_DIR / "08_plants_matched.geo.parquet"
        matched.to_parquet(out, index=False)

        return str(out)

    @task
    def create_connections(plants_matched_path: str, geo_paths: dict[str, str]) -> str:
        import geopandas as gpd

        from src.map import create_connection_lines

        gdf_plants = gpd.read_parquet(plants_matched_path)
        gdf_substations = gpd.read_parquet(geo_paths["substations"])

        connections = create_connection_lines(gdf_plants, gdf_substations)

        out = WORK_DIR / "09_connection_lines.geo.parquet"
        connections.to_parquet(out, index=False)

        return str(out)

    @task
    def summarize_substations(
        plants_matched_path: str, geo_paths: dict[str, str]
    ) -> str:
        import geopandas as gpd

        from src.map import build_substation_summary

        gdf_plants = gpd.read_parquet(plants_matched_path)
        gdf_substations = gpd.read_parquet(geo_paths["substations"])

        summary = build_substation_summary(gdf_plants, gdf_substations)

        out = WORK_DIR / "10_substation_summary.geo.parquet"
        summary.to_parquet(out, index=False)

        return str(out)

    @task
    def export_outputs(
        plants_matched_path: str,
        substation_summary_path: str,
        connection_lines_path: str,
    ) -> dict[str, str]:
        import geopandas as gpd

        from src.export import save_results_to_geopackage, visualize_results

        gdf_plants = gpd.read_parquet(plants_matched_path)
        gdf_substations = gpd.read_parquet(substation_summary_path)
        gdf_connections = gpd.read_parquet(connection_lines_path)

        gpkg_path = OUTPUT_DIR / "solution.gpkg"
        html_path = OUTPUT_DIR / "interactive_map.html"

        save_results_to_geopackage(
            gpkg_path,
            gdf_plants,
            gdf_substations,
            gdf_connections,
        )

        visualize_results(
            html_path,
            gdf_plants,
            gdf_substations,
            gdf_connections,
        )

        return {
            "geopackage": str(gpkg_path),
            "map": str(html_path),
        }

    prepare = prepare_directories()

    mastr_raw = ingest_mastr_plants()
    osm_raw = ingest_osm_substations()

    prepare >> [mastr_raw, osm_raw]

    plants = transform_plants(mastr_raw)
    modeled_plants = model_energy(plants)

    substations = transform_substations(osm_raw)

    geo_paths = convert_to_geo(modeled_plants, substations)
    matched_plants = match_spatially(geo_paths)

    connections = create_connections(matched_plants, geo_paths)
    summary = summarize_substations(matched_plants, geo_paths)

    export = export_outputs(matched_plants, summary, connections)
