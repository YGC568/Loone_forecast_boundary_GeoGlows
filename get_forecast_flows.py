from loone_data_prep.flow_data.forecast_bias_correction import get_bias_corrected_data
from loone_data_prep.utils import get_dbkeys
import datetime
import geoglows
import os
import sys
import glob
import pandas as pd
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger
import tempfile

# Silence rpy2 warnings
rpy2_logger.setLevel('ERROR')

STATION_IDS = [
    "S191_S", "S65E_S", "S65EX1_S", "S84_S", "S154_C", "S71_S", "S72_S",
    "FISHP", "S308.DS", "L8.441", "S133_P", "S127_C", "S127_P", "S129_C",
    "S135_C", "S2_P", "S3_P", "S4_P", "S351_S", "S352_S", "S354_S",
    "S129 PMP_P", "S135 PMP_P", "S77_S", "INDUST", "S79_S", "S80_S",
    "S40_S", "S49_S"
]

REACH_IDS = {
    "S191_S": 13082707, "S65E_S": 13082699, "S65EX1_S": 13082699,
    "S84_S": 13082700, "S154_C": 13082716, "S71_S": 13082743, "S72_S": 13082727,
    "FISHP": 13082756, "S308.DS": 13082736, "L8.441": 13082747, "S133_P": 13082709,
    "S127_C": 13082716, "S127_P": 13082716, "S129_C": 13082727, "S135_C": 13082725,
    "S2_P": 13082783, "S3_P": 13082809, "S4_P": 13082806, "S351_S": 13082804,
    "S352_S": 13082762, "S354_S": 13082809, "S129 PMP_P": 13082727, "S135 PMP_P": 13082725,
    "S77_S": 13082767, "INDUST": 13082806, "S79_S": 13082791, "S80_S": 13082718,
    "S40_S": 13082797, "S49_S": 13082696
}

SECONDS_IN_HOUR = 3600
SECONDS_IN_DAY = 86400
HOURS_IN_DAY = 24

FORECAST_DATE = (datetime.datetime.now()).strftime("%Y%m%d")

GEOGLOWS_ENDPOINT = "https://geoglows.ecmwf.int/api/"
#Change new api endpoint version2 and no change in other code lines.

def get_stations_latitude_longitude(station_ids: list[str]):
    """Gets the latitudes and longitudes of the given stations.

    Args:
        station_ids (list[str]): The ids of the stations to get the latitudes/longitudes of

    Returns:
        (dict[str, tuple[float, float]]): A dictionary of format dict<station_id:(latitude,longitude)>
    """
    station_data = {}

    try:
        # Get the station/dbkey data
        r_dataframe = get_dbkeys(
            station_ids=station_ids,
            category="SW",
            param="",
            stat="",
            recorder="",
            detail_level="full",
        )

        # Convert the r dataframe to a pandas dataframe
        with ro.conversion.localconverter(ro.default_converter + pandas2ri.converter):
            pd_dataframe = pandas2ri.rpy2py(r_dataframe)

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, 'temp_dataframe.csv')

            # Save to CSV with encoding 'utf-8' and read back to handle encoding
            pd_dataframe.to_csv(temp_file_path, encoding='utf-8')
            pd_dataframe = pd.read_csv(temp_file_path, encoding='utf-8')

        # Debug print to check the type and structure of pd_dataframe
        print(f"Type of pd_dataframe: {type(pd_dataframe)}")
        print(pd_dataframe.head())

        # Ensure pd_dataframe is a pandas DataFrame
        if not isinstance(pd_dataframe, pd.DataFrame):
            raise TypeError(
                "The converted R dataframe is not a pandas DataFrame")

        # Filter out extra rows for each station from the dataframe
        pd_dataframe.drop_duplicates(
            subset="Station", keep="first", inplace=True)

        # Get latitude/longitude of each station
        for index in pd_dataframe.index:
            station = pd_dataframe["Station"][index]
            latitude = pd_dataframe["Latitude"][index]
            longitude = pd_dataframe["Longitude"][index]
            station_data[station] = latitude, longitude

    except UnicodeDecodeError as e:
        print(f"UnicodeDecodeError: {e}")

    return station_data


def get_reach_id(latitude: float, longitude: float):
    """Gets the reach id for the given latitude/longitude.

    Args:
        latitude (float): The latitude to retrieve the reach id of
        longitude (float): The longitude to retrieve the reach id of

    Returns:
        (int): The reach id of the given latitude/longitude
    """
    reach_data = geoglows.streamflow.latlon_to_reach(latitude, longitude)

    if "error" in reach_data:
        raise Exception(reach_data["error"])

    return reach_data["reach_id"]


def get_flow_forecast_ensembles(reach_id: str, forecast_date: str):
    """Gets the 52 ensemble forecasts from geoglows for the given reach_id.

    Args:
        reach_id (str): The reach_id to get the 52 ensemble forecasts for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format

    Returns:
        (pandas.core.frame.DataFrame): The 52 ensemble flow forecasts.
    """
    return geoglows.streamflow.forecast_ensembles(
        reach_id=reach_id, forecast_date=forecast_date, endpoint=GEOGLOWS_ENDPOINT
    )


def get_flow_forecast_stats(reach_id: str, forecast_date: str):
    """Gets the 15-day time series forecast stats (max, min, etc) from geoglows for the given reach_id.

    Args:
        reach_id (str): The reach_id to get the forecast stats for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format

    Returns:
        (pandas.core.frame.DataFrame): The forecast stats
    """
    return geoglows.streamflow.forecast_stats(
        reach_id=reach_id, forecast_date=forecast_date, endpoint=GEOGLOWS_ENDPOINT
    )


def ensembles_to_csv(
    workspace: str,
    station_id: str,
    ensembles: pd.core.frame.DataFrame,
    stats: pd.core.frame.DataFrame,
    date: str
):
    """Writes the ensembles and stats from the given DataFrames to a file .csv file.
        Each ensemble and stat is written in its own column.
        The name of each file is of the format: <station_id>_<date>_FLOW_cmd_geoglows.csv

    Args:
        workspace (str): The path to the directory to write the files out to.
        station_id (str): The id of the station that the data is from.
        ensembles (pandas.core.frame.DataFrame): The DataFrame that holds the flow data.
        stats (pandas.core.frame.DataFrame): The DataFrame that holds the stats data.
        date (str): The date to include in the file name.
    """
    # Get the path to the file that will be written
    file_name = f"{station_id}_FLOW_cmd_geoglows_{date}.csv"
    file_path = os.path.join(workspace, file_name)

    # Format DataFrames for LOONE
    ensembles = _format_ensembles_DataFrame(ensembles)
    stats = _format_stats_DataFrame(stats)

    # Append columns in stats to ensembles
    for column_name in stats.columns:
        ensembles[column_name] = stats[column_name].tolist()

    # Write out the .csv file
    ensembles.to_csv(file_path, encoding='utf-8')

    # Notify user of success
    print(f"File Saved: {file_path}")


def _format_ensembles_DataFrame(dataframe: pd.core.frame.DataFrame):
    """Formats, modifies, and returns the given pandas DataFrame's data to a format that LOONE expects.
        The given DataFrame should hold the ensembles retrieved from geoglows.
        Meant to be used as a helper function in ensembles_to_csv().

    Args:
        dataframe (pandas.core.frame.DataFrame):

    Returns:
        (pandas.core.frame.DataFrame): The resulting formatted/modified pandas DataFrame.
    """
    # Remove high resolution columns (ensemble 52)
    if "ensemble_52_m^3/s" in dataframe.columns:
        dataframe.drop(columns="ensemble_52_m^3/s", inplace=True)

    # Remove rows with null values
    dataframe.dropna(axis="index", inplace=True)

    # Make all times in datetimes 00:00:00+00:00 (ignore time, only use date)
    dataframe.index = dataframe.index.normalize()

    # Convert m^3/s data to m^3/h
    dataframe = dataframe.transform(lambda x: x * SECONDS_IN_HOUR)

    # Make negative values 0
    dataframe.clip(0, inplace=True)

    # Get the average m^3/d for each day
    dataframe = dataframe.groupby([dataframe.index]).mean()
    dataframe = dataframe.transform(lambda x: x * HOURS_IN_DAY)

    # Format datetimes to just dates
    dataframe.index = dataframe.index.strftime("%Y-%m-%d")

    # Rename columns to *_m^3/d from *_m^3/s
    column_names = []
    for column_name in dataframe.columns:
        column_names.append(column_name.replace("m^3/s", "m^3/d"))

    dataframe.columns = column_names

    # Rename index from datetimes to date
    dataframe.rename_axis("date", inplace=True)

    # Return resulting DataFrame
    return dataframe


def _format_stats_DataFrame(dataframe: pd.core.frame.DataFrame):
    """Formats, modifies, and returns the given pandas DataFrame's data to a format that LOONE expects.
        The given DataFrame should hold the stats retrieved from geoglows.
        Meant to be used as a helper function in ensembles_to_csv().

    Args:
        dataframe (pandas.core.frame.DataFrame):

    Returns:
        (pandas.core.frame.DataFrame): The resulting formatted/modified pandas DataFrame.
    """
    # Remove high resolution columns (ensemble 52, high_res_m^3/s)
    if "high_res_m^3/s" in dataframe.columns:
        dataframe.drop(columns="high_res_m^3/s", inplace=True)

    # Remove rows with null values
    dataframe.dropna(axis="index", inplace=True)

    # Make all times in datetimes 00:00:00+00:00 (ignore time, only use date)
    dataframe.index = dataframe.index.normalize()

    # Convert m^3/s data to m^3/h
    dataframe = dataframe.transform(lambda x: x * SECONDS_IN_HOUR)

    # Make negative values 0
    dataframe.clip(0, inplace=True)

    # Max Column (Max)
    column_max = dataframe[["flow_max_m^3/s"]].copy()
    column_max = column_max.groupby([column_max.index]).max()

    # 75th Percentile Column (Average)
    column_75percentile = dataframe[["flow_75%_m^3/s"]].copy()
    column_75percentile = column_75percentile.groupby(
        [column_75percentile.index]).mean()

    # Average Column (Weighted Average)
    column_average = dataframe[["flow_avg_m^3/s"]].copy()
    column_average.transform(lambda x: x / 8)
    column_average = column_average.groupby([column_average.index]).sum()

    # 25th Percentile Column (Average)
    column_25percentile = dataframe[["flow_25%_m^3/s"]].copy()
    column_25percentile = column_25percentile.groupby(
        [column_25percentile.index]).mean()

    # Min Column (Min)
    column_min = dataframe[["flow_min_m^3/s"]].copy()
    column_min = column_min.groupby([column_min.index]).min()

    # Convert values in each column from m^3/h to m^3/d
    column_max = column_max.transform(lambda x: x * HOURS_IN_DAY)
    column_75percentile = column_75percentile.transform(
        lambda x: x * HOURS_IN_DAY)
    column_average = column_average.transform(lambda x: x * HOURS_IN_DAY)
    column_25percentile = column_25percentile.transform(
        lambda x: x * HOURS_IN_DAY)
    column_min = column_min.transform(lambda x: x * HOURS_IN_DAY)

    # Append modified columns into one pandas DataFrame
    dataframe_result = pd.DataFrame()
    dataframe_result.index = dataframe.groupby([dataframe.index]).mean().index
    dataframe_result["flow_max_m^3/d"] = column_max["flow_max_m^3/s"].tolist()
    dataframe_result["flow_75%_m^3/d"] = column_75percentile["flow_75%_m^3/s"].tolist()
    dataframe_result["flow_avg_m^3/d"] = column_average["flow_avg_m^3/s"].tolist()
    dataframe_result["flow_25%_m^3/d"] = column_25percentile["flow_25%_m^3/s"].tolist()
    dataframe_result["flow_min_m^3/d"] = column_min["flow_min_m^3/s"].tolist()

    # Format datetimes to just dates
    dataframe_result.index = dataframe_result.index.strftime("%Y-%m-%d")

    # Rename index from datetimes to date
    dataframe_result.rename_axis("date", inplace=True)

    # Return resulting DataFrame
    return dataframe_result


def main(
    workspace: str,
    station_ids: list[str] = STATION_IDS,
    forecast_date: str = FORECAST_DATE,
    bias_corrected: bool = False,
    observed_data_dir: str | None = None,
    cache_path: str | None = None,
):
    """Downloads the flow forecasts for the given station ids and writes them out as .csv files.

    Args:
        workspace (str): Where to write the .csv files to.
        station_ids (list[str]): The station ids to get the flow data for.
        forecast_date (str): A string specifying the date to request in YYYYMMDD format.
        bias_corrected (bool): Whether or not to use bias corrected data. Default is False.
        observed_data_dir (str): The path to the observed flow data directory (only needed if bias corrected is True).
        cache_path (str): The path to the cache directory for geoglows data. Should hold a directory named geoglows_cache that holds the cached files. Use None to not use a cache.
    """
    reach_ids = {}

    # Get the latitude/longitude for each station
    station_locations = get_stations_latitude_longitude(station_ids)

    # Check for any download failures
    for station_id in station_ids:
        if station_id in REACH_IDS.keys():
            reach_ids[station_id] = REACH_IDS[station_id]
        elif station_id not in station_locations.keys():
            print(f"Error: The longitude and latitude could not be downloaded for station {station_id}")

    # Get station reach ids
    for station_id in station_locations.keys():
        if station_id not in reach_ids.keys():
            location = station_locations[station_id]
            try:
                reach_ids[station_id] = get_reach_id(location[0], location[1])
            except Exception as e:
                print(f"Error: Failed to get reach id for station {station_id} ({str(e)})")

    # Get the flow data for each station
    for station_id in reach_ids.keys():
        reach_id = reach_ids[station_id]
        station_ensembles = get_flow_forecast_ensembles(reach_id, forecast_date)
        station_stats = get_flow_forecast_stats(reach_id, forecast_date)

        if bias_corrected:
            observed_data_list = glob.glob(
                os.path.join(observed_data_dir, f"{station_id}*FLOW_cmd.csv")
            )
            if observed_data_list:
                observed_data_path = observed_data_list[0]
                station_ensembles, station_stats = get_bias_corrected_data(
                    station_id,
                    reach_id,
                    observed_data_path,
                    station_ensembles,
                    station_stats,
                    cache_path,
                )

        ensembles_to_csv(workspace, station_id, station_ensembles, station_stats, forecast_date)


if __name__ == "__main__":
    workspace = sys.argv[1].rstrip("/")
    bias_corrected = sys.argv[2].lower() in ["true", "yes", "y", "1"]
    observed_data_path = sys.argv[3]
    main(workspace, bias_corrected=bias_corrected, observed_data_dir=observed_data_path)
