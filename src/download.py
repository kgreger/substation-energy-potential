import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Overpass QL query for Brandenburg substations. Includes power=substation (nodes, ways, and relations), but excludes substation=minor_distribution query level.
OVERPASS_QUERY = """
    [out:json][timeout:180];
    area
      ["boundary"="administrative"]
      ["admin_level"="4"]
      ["name"="Brandenburg"]
      ->.searchArea;

    (
      node
        ["power"="substation"]
        ["substation"!="minor_distribution"]
        ["location"!="kiosk"]
        (area.searchArea);
      way
        ["power"="substation"]
        ["substation"!="minor_distribution"]
        ["location"!="kiosk"]
        (area.searchArea);
      relation
        ["power"="substation"]
        ["substation"!="minor_distribution"]
        ["location"!="kiosk"]
        (area.searchArea);
    );
    out center tags;
    """


def is_file_from_today(path: str | Path) -> bool:
    """
    Check if the modified date of a file is today.

    Args:
        path (str | Path): a path to a file

    Returns:
        bool
    """

    path = Path(path)

    if not path.exists():
        return False

    modified_date = datetime.fromtimestamp(path.stat().st_mtime).date()
    today = datetime.today().date()

    return modified_date == today


def fetch_mastr_data(
    base_url: str, max_pages: int = 1000, sleep: float = 0.2
) -> pd.DataFrame:
    """
    Fetch paginated MaStR data until no more records are returned.

    Args:
        base_url (str): URL with page=1 included (will be replaced dynamically)
        max_pages (int): safety cap to avoid infinite loops
        sleep (float): delay between requests (seconds)

    Returns:
        pd.DataFrame: all records combined as a DataFrame
    """

    df = []
    page = 1
    session = requests.Session()

    while page <= max_pages:
        url = base_url.replace("page=1", f"page={page}")

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

        except Exception as e:
            print(f"Error on page {page}: {e}")
            break

        records = data.get("Data", [])

        if not records:
            print(f"No more data at page {page}. Stopping.")
            break

        df.extend(records)
        print(f"Fetched page {page} ({len(records):,.0f} records).")

        page += 1
        time.sleep(sleep)

    print(f"\nTotal records fetched: {len(df):,.0f}")

    return df


def load_or_fetch_mastr_data(base_url: str, parquet_path: str | Path) -> pd.DataFrame:
    """
    Fetch all MaStR data from the web, unless a cached version from today exists locally.

    Args:
        base_url (str): URL with page=1 included (will be replaced dynamically)
        parquet_path (str | Path): path of a locally stored parquet cache

    Returns:
        pd.DataFrame: all records combined as a DataFrame
    """

    parquet_path = Path(parquet_path)

    if is_file_from_today(parquet_path):
        print(f"Loading MaStR plant data from parquet {parquet_path} (cached today)...")
        return pd.read_parquet(parquet_path)

    print("Fetching fresh data from MaStR...")

    df_mastr = fetch_mastr_data(base_url)

    df = pd.json_normalize(df_mastr)

    df.to_parquet(parquet_path, index=False)

    print(f"Saved fresh plant MaStRdata to {parquet_path}.")

    return df


def fetch_overpass_json(
    query: str = OVERPASS_QUERY,
    overpass_url: str = OVERPASS_URL,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> dict[str, Any]:
    """
    Execute an Overpass query with simple retry logic and return the JSON response.

    Args:
        query (str): the Overpass QL query to execute
        overpass_url (str): the URL of the Overpass API endpoint
        max_retries (int): maximum number of retry attempts in case of failure
        backoff_seconds (float): base number of seconds to wait between retries

    Returns:
        dict[str, Any]: the JSON response from the Overpass API
    """

    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                overpass_url,
                data={"data": query},
                timeout=(30, 300),
                headers={"User-Agent": "substation-energy-potential/1.0"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)
            else:
                raise RuntimeError(
                    f"Overpass request failed after {max_retries} attempts."
                ) from last_error

    raise RuntimeError("Unexpected Overpass fetch failure.")


def overpass_elements_to_dataframe(payload: dict[str, Any]) -> pd.DataFrame:
    """
    Convert Overpass elements into a flat DataFrame.

    Args:
        payload (dict[str, Any]): the JSON response from the Overpass API containing "elements"

    Returns:
        pd.DataFrame: a DataFrame where each row corresponds to an OSM element with relevant tags and metadata
    """

    rows: list[dict[str, Any]] = []

    for element in payload.get("elements", []):
        tags = element.get("tags", {})

        lat = element.get("lat")
        lon = element.get("lon")

        # ways / relations come back with "center"
        if lat is None or lon is None:
            center = element.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        rows.append(
            {
                "osm_type": element.get("type"),
                "osm_id": element.get("id"),
                "substation_id": element.get("type") + "_" + str(element.get("id")),
                "lat": lat,
                "lon": lon,
                "name": tags.get("name"),
                "operator": tags.get("operator"),
                "ref": tags.get("ref"),
                "power": tags.get("power"),
                "substation_type": tags.get("substation"),
                "location": tags.get("location"),
                "voltage": tags.get("voltage"),
            }
        )

    return pd.DataFrame(rows)


def fetch_brandenburg_substations() -> pd.DataFrame:
    """
    Fetch all Brandenburg substation data using Overpass API and keep only medium/high-voltage substations.

    Args:
        None

    Returns:
        pd.DataFrame: all records combined as a DataFrame
    """

    return overpass_elements_to_dataframe(fetch_overpass_json())


def load_or_fetch_osm_data(parquet_path: str | Path) -> pd.DataFrame:
    """
    Fetch all OSM data using Overpass API, unless a cached version from today exists locally.

    Args:
        parquet_path (str | Path): path of a locally stored parquet cache

    Returns:
        pd.DataFrame: all records combined as a DataFrame
    """

    parquet_path = Path(parquet_path)

    if is_file_from_today(parquet_path):
        print(
            f"Loading OSM substation data from parquet {parquet_path} (cached today)..."
        )
        return pd.read_parquet(parquet_path)

    print("Fetching fresh data from OSM...")
    df_substations = fetch_brandenburg_substations()
    df_substations.to_parquet(parquet_path, index=False)
    print(f"Saved fresh OSM substation data to {parquet_path}.")

    return df_substations
