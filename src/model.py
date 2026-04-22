import pandas as pd


def model_yield(plants: pd.DataFrame, yields: dict[str, float]) -> pd.DataFrame:
    """
    Estimate annual energy yield for each plant based on technology type and capacity.

    Args:
        plants (pd.DataFrame): DataFrame containing plant information with 'technology' and 'capacity_mw' columns
        yields (dict): Dictionary mapping technology types to their respective energy yields (MWh/MW)

    Returns:
        pd.DataFrame: DataFrame with estimated annual energy yield for each plant
    """
    plants = plants.copy()

    plants["estimated_annual_energy_mwh"] = plants["capacity_mw"] * plants[
        "technology"
    ].map(yields)

    return plants
