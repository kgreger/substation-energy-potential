from typing import Any

import numpy as np
import pandas as pd


def parse_mastr_date_series(series: pd.Series) -> pd.Series:
    """
    Convert raw MaStR date strings to datetime objects.

    Args:
        series (pd.Series): Series containing raw MaStR date strings

    Returns:
        pd.Series: Series with converted datetime objects
    """

    extracted = series.astype("string").str.extract(r"/Date\((\d+)\)/", expand=False)
    return pd.to_datetime(extracted, unit="ms", errors="coerce")


def transform_plants_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw MaStR data, filter for relevant assets, convert kW to MW, strip unnecessary columns.

    Args:
        df_raw (pd.DataFrame): Raw MaStR data as a DataFrame

    Returns:
        pd.DataFrame: Transformed and filtered data as a DataFrame
    """

    df = df_raw.copy()

    # ensure correct data types for relevant columns
    df["EnergietraegerName"] = df["EnergietraegerName"].astype("string").str.strip()
    df["Nettonennleistung"] = pd.to_numeric(df["Nettonennleistung"], errors="coerce")
    df["Breitengrad"] = pd.to_numeric(df["Breitengrad"], errors="coerce")
    df["Laengengrad"] = pd.to_numeric(df["Laengengrad"], errors="coerce")

    # define boolean masks for filtering
    is_solar = df["EnergietraegerName"].eq("Solare Strahlungsenergie")
    is_wind = df["EnergietraegerName"].eq("Wind")
    is_onshore_wind = df["WindAnLandOderSeeBezeichnung"].eq("Windkraft an Land")
    is_rooftop_solar = df["ArtDerSolaranlageBezeichnung"].eq("Gebäudesolaranlage")
    is_utility_scale_solar = (
        is_solar & df["Nettonennleistung"].ge(1000) & ~is_rooftop_solar
    )
    is_relevant_asset = (is_wind & is_onshore_wind) | is_utility_scale_solar
    has_required_geo_and_capacity = (
        df["Breitengrad"].notna()
        & df["Laengengrad"].notna()
        & df["Nettonennleistung"].notna()
        & df["Nettonennleistung"].gt(0)
    )

    # filter the DataFrame based on the defined criteria
    df_filtered = df.loc[is_relevant_asset & has_required_geo_and_capacity].copy()

    # assign technology type based on energy carrier
    df_filtered["technology"] = np.where(
        df_filtered["EnergietraegerName"].eq("Wind"),
        "wind",
        "solar",
    )

    # create new columns with standardized names and formats
    df_filtered["plant_id"] = df_filtered["MaStRNummer"]
    df_filtered["plant_name"] = df_filtered["EinheitName"]
    df_filtered["capacity_mw"] = df_filtered["Nettonennleistung"] / 1000
    df_filtered["lat"] = df_filtered["Breitengrad"]
    df_filtered["lon"] = df_filtered["Laengengrad"]
    # df_filtered["commissioning_date"] = parse_mastr_date_series(
    #     df_filtered["InbetriebnahmeDatum"]
    # )
    df_filtered["municipality"] = df_filtered["Gemeinde"]
    df_filtered["postal_code"] = df_filtered["Plz"]
    df_filtered["county"] = df_filtered["Landkreis"]
    df_filtered["state"] = df_filtered["Bundesland"]
    # df_filtered["operator_name"] = df_filtered["AnlagenbetreiberName"]
    # df_filtered["operator_id"] = df_filtered["AnlagenbetreiberMaStRNummer"]
    # df_filtered["grid_operator_name"] = df_filtered["NetzbetreiberNamen"]
    # df_filtered["grid_operator_id"] = df_filtered["NetzbetreiberMaStRNummer"]
    df_filtered["voltage_level"] = df_filtered["SpannungsebenenNamen"]
    # df_filtered["location_id"] = df_filtered["LokationMastrNr"]
    # df_filtered["address"] = df_filtered["StandortAnonymisiert"]
    # df_filtered["site_name"] = df_filtered["WindparkName"].fillna(
    #     df_filtered["SolarparkName"]
    # )
    df_filtered["solar_type"] = df_filtered["ArtDerSolaranlageBezeichnung"]
    df_filtered["wind_location_type"] = df_filtered["WindAnLandOderSeeBezeichnung"]
    # df_filtered["wind_turbine_type"] = df_filtered["Typenbezeichnung"]
    # df_filtered["wind_manufacturer"] = df_filtered[
    #     "HerstellerWindenergieanlageBezeichnung"
    # ]
    # df_filtered["wind_hub_height_m"] = pd.to_numeric(
    #     df_filtered["NabenhoeheWindenergieanlage"], errors="coerce"
    # )
    # df_filtered["wind_rotor_diameter_m"] = pd.to_numeric(
    #     df_filtered["RotordurchmesserWindenergieanlage"], errors="coerce"
    # )

    # df_filtered["full_or_partial_feed_in"] = df_filtered[
    #     "VollTeilEinspeisungBezeichnung"
    # ]
    # df_filtered["operating_status"] = df_filtered["BetriebsStatusName"]
    # df_filtered["system_status"] = df_filtered["SystemStatusName"]

    # assign boolean masks
    df_filtered["is_wind"] = df_filtered["technology"].eq("wind")
    df_filtered["is_solar"] = df_filtered["technology"].eq("solar")
    df_filtered["is_onshore_wind"] = df_filtered["wind_location_type"].eq(
        "Windkraft an Land"
    )
    df_filtered["is_rooftop_solar"] = df_filtered["solar_type"].eq("Gebäudesolaranlage")
    df_filtered["is_utility_scale_solar"] = (
        df_filtered["is_solar"]
        & df_filtered["capacity_mw"].ge(1.0)
        & ~df_filtered["is_rooftop_solar"]
    )
    df_filtered["is_relevant_asset"] = True

    # identify and keep only columns that are relevant for the analysis and visualization
    relevant_columns = [
        "plant_id",
        "plant_name",
        "technology",
        "capacity_mw",
        "lat",
        "lon",
        # "location_id",
        # "address",
        "postal_code",
        "municipality",
        "county",
        # "state",
        # "commissioning_date",
        # "site_name",
        # "operator_name",
        # "operator_id",
        # "grid_operator_name",
        # "grid_operator_id",
        # "voltage_level",
        # "full_or_partial_feed_in",
        "solar_type",
        "wind_location_type",
        # "operating_status",
        # "system_status",
        "is_solar",
        "is_wind",
        "is_onshore_wind",
        "is_rooftop_solar",
        "is_utility_scale_solar",
        "is_relevant_asset",
    ]

    print(f"Raw rows: {len(df_raw):,.0f}")
    print(f"Relevant rows: {len(df_filtered):,.0f}")
    print(df_filtered["technology"].value_counts(dropna=False))

    return df_filtered[relevant_columns].copy()


def parse_voltage_to_list(voltage_value: Any) -> list[int]:
    """
    Parse OSM "voltage" tag values into a list of integers in Volts.

    Examples:
    - '110000' -> [110000]
    - '110000;20000' -> [110000, 20000]
    - None -> []

    Args:
        voltage_value (Any): the raw value of the OSM "voltage" tag, which can be a string with one or more voltage levels separated by semicolons, or it can be missing/null

    Returns:
        list[int]: a list of voltage levels in Volts as integers, or an empty list if the input is None or cannot be parsed
    """

    if voltage_value is None or pd.isna(voltage_value):
        return []

    values: list[int] = []
    for part in str(voltage_value).split(";"):
        cleaned = part.strip()
        if not cleaned:
            continue
        try:
            values.append(int(float(cleaned)))
        except ValueError:
            continue
    return values


def transform_substation_data(
    df_raw: pd.DataFrame,
    min_voltage_v: int = 20_000,
) -> pd.DataFrame:
    """
    Keep only substations with a tagged highest voltage >= min_voltage_v to focus on medium- and high-voltage substations.
    Also excludes rows without coordinates.

    Args:
        df_raw (pd.DataFrame): DataFrame containing OSM substation data with a "voltage" column (as string)
        min_voltage_v (int): minimum voltage in Volts (e.g. 20,000 for 20kV) to keep

    Returns:
        pd.DataFrame: filtered DataFrame with only medium/high-voltage substations and valid coordinates
    """
    df = df_raw.copy()

    df["voltage_levels_v"] = df["voltage"].apply(parse_voltage_to_list)
    df["max_voltage_v"] = df["voltage_levels_v"].apply(
        lambda vals: max(vals) if vals else pd.NA
    )

    df = df.rename(
        columns={
            "name": "substation_name",
        }
    )

    df_filtered = df.loc[
        df["lat"].notna()
        & df["lon"].notna()
        & df["max_voltage_v"].notna()
        & df["max_voltage_v"].ge(min_voltage_v)
    ].copy()

    print(f"Raw rows: {len(df_raw):,.0f}")
    print(f"Relevant rows: {len(df_filtered):,.0f}")

    return df_filtered.reset_index(drop=True).copy()
