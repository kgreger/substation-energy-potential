from pathlib import Path

import folium
import geopandas as gpd
from folium.features import RegularPolygonMarker


def save_results_to_geopackage(
    gpkg_path: str | Path,
    gdf_plants: gpd.GeoDataFrame,
    gdf_substations: gpd.GeoDataFrame,
    gdf_connections: gpd.GeoDataFrame,
) -> None:
    """
    Save the results of the substation energy potential analysis to a GeoPackage file.

    Args:
        gdf_plants (gpd.GeoDataFrame): GeoDataFrame containing plant data with assigned substation information and estimated annual energy yield
        gdf_substations (gpd.GeoDataFrame): GeoDataFrame containing substation data
        gdf_connections (gpd.GeoDataFrame): GeoDataFrame containing connection lines between plants and substations

    Returns:
        None
    """

    gpkg_path = Path(gpkg_path)

    for gdf, layer in [
        (gdf_plants.to_crs("EPSG:4326"), "plants"),
        (gdf_substations.to_crs("EPSG:4326"), "substations"),
        (gdf_connections.to_crs("EPSG:4326"), "plant_substation_connections"),
    ]:
        gdf.to_file(gpkg_path, layer=layer, driver="GPKG")

    print(f"Results saved to GeoPackage {gpkg_path}.")


def visualize_results(
    html_path: str | Path,
    gdf_plants: gpd.GeoDataFrame,
    gdf_substations: gpd.GeoDataFrame,
    gdf_connections: gpd.GeoDataFrame,
) -> None:
    """
    Visualize the results of the substation energy potential analysis on an interactive map.

    Args:
        gdf_plants (gpd.GeoDataFrame): GeoDataFrame containing plant data with assigned substation information and estimated annual energy yield
        gdf_substations (gpd.GeoDataFrame): GeoDataFrame containing substation data
        gdf_connections (gpd.GeoDataFrame): GeoDataFrame containing connection lines between plants and substations

    Returns:
        None
    """

    html_path = Path(html_path)

    gdf_plants_map = gdf_plants.to_crs("EPSG:4326").copy()
    gdf_substations_map = gdf_substations.to_crs("EPSG:4326").copy()
    gdf_connections_map = gdf_connections.to_crs("EPSG:4326").copy()

    minx, miny, maxx, maxy = gdf_substations_map.total_bounds
    center = [(miny + maxy) / 2, (minx + maxx) / 2]

    m = folium.Map(
        location=center,
        zoom_start=7,
        tiles=None,
    )
    m.fit_bounds([[miny, minx], [maxy, maxx]])

    folium.TileLayer(
        tiles="OpenStreetMap",
        name="OpenStreetMap",
        control=True,
    ).add_to(m)

    folium.TileLayer(
        tiles="CartoDB positron",
        name="Light map",
        control=True,
    ).add_to(m)

    folium.TileLayer(
        tiles="Esri.WorldImagery",
        name="Satellite",
        control=True,
    ).add_to(m)

    plants_layer = folium.FeatureGroup(name="Plants")
    substations_layer = folium.FeatureGroup(name="Substations")
    lines_layer = folium.FeatureGroup(name="Connections")

    for _, row in gdf_plants_map.iterrows():
        color = "blue" if row["technology"] == "wind" else "orange"

        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=min(8, max(2, row["capacity_mw"] / 5)),
            color=color,
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(
                f"""
                <h1>{row["plant_name"]}</h1>
                <b>Plant ID:</b> {row["plant_id"]}<br>
                <b>Type:</b> {row["technology"].capitalize()}<br>
                <br>
                <b>Capacity (MW):</b> {row["capacity_mw"]:,.2f} MW<br>
                <b>Estimated annual energy (MWh):</b> {row["estimated_annual_energy_mwh"]:,.0f} MWh<br>
                """,
                max_width=250,
            ),
        ).add_to(plants_layer)

    for _, row in gdf_substations_map.iterrows():
        RegularPolygonMarker(
            location=[row.geometry.y, row.geometry.x],
            number_of_sides=4,
            radius=6,
            color="black",
            fill=True,
            fill_color="black",
            fill_opacity=0.95,
            popup=folium.Popup(
                f"""
                <h1>{row["substation_name"]}</h1>
                <b>Substation ID:</b> {row["substation_id"]}<br>
                <b>Voltage:</b> {row["max_voltage_v"]:,.0f} V<br>
                <br>
                <b>Wind capacity:</b> {row["wind_capacity_mw"]:,.2f} MW<br>
                <b>Wind estimated annual energy:</b> {row["wind_estimated_annual_energy_mwh"]:,.0f} MWh<br>
                <br>
                <b>Solar capacity:</b> {row["solar_capacity_mw"]:,.2f} MW<br>
                <b>Solar estimated annual energy:</b> {row["solar_estimated_annual_energy_mwh"]:,.0f} MWh<br>
                <br>
                <b>Total capacity:</b> {row["total_capacity_mw"]:,.2f} MW<br>
                <b>Total estimated annual energy:</b> {row["total_estimated_annual_energy_mwh"]:,.0f} MWh<br>
                """,
                max_width=250,
            ),
        ).add_to(substations_layer)

    for _, row in gdf_connections_map.iterrows():
        coords = [(lat, lon) for lon, lat in row.geometry.coords]

        folium.PolyLine(
            locations=coords,
            color="black",
            weight=2,
            opacity=0.7,
        ).add_to(lines_layer)

    lines_layer.add_to(m)
    substations_layer.add_to(m)
    plants_layer.add_to(m)

    folium.LayerControl().add_to(m)

    legend_html = """
        <div style="
            position: fixed;
            bottom: 40px;
            left: 40px;
            width: 190px;
            z-index: 9999;
            font-size: 14px;
            background-color: white;
            border: 2px solid grey;
            border-radius: 6px;
            padding: 10px;
            box-shadow: 3px 3px 8px rgba(0,0,0,0.3);
        ">
            <b>Legend</b><br>
            <div style="margin-top: 8px;">
                <span style="
                    display: inline-block;
                    width: 10px;
                    height: 10px;
                    border-radius: 50%;
                    background: blue;
                    margin-right: 8px;
                "></span>
                Wind plant
            </div>
            <div style="margin-top: 6px;">
                <span style="
                    display: inline-block;
                    width: 10px;
                    height: 10px;
                    border-radius: 50%;
                    background: orange;
                    margin-right: 8px;
                "></span>
                Solar plant
            </div>
            <div style="margin-top: 6px;">
                <span style="
                    display: inline-block;
                    width: 10px;
                    height: 10px;
                    background: black;
                    margin-right: 8px;
                "></span>
                Substation
            </div>
            <div style="margin-top: 6px;">
                <span style="
                    display: inline-block;
                    width: 18px;
                    height: 2px;
                    background: grey;
                    margin-right: 8px;
                    vertical-align: middle;
                "></span>
                Connection line
            </div>
        </div>
        """
    m.get_root().html.add_child(folium.Element(legend_html))

    m.save(html_path)

    print(f"Interactive map saved to {html_path}.")
