import itertools

import pandas as pd
import requests

from task_geo.dataset_builders.nasa.references import PARAMETERS
from task_geo.dataset_builders.nasa.area_partition import area_partition


def nasa_data_loc(lat, lon, str_start_date, str_end_date, parms_str):
    """
    Extract data for a single location.

    Parameters
    ----------
    lat : string
    lon : string
    str_start_date : string
    str_end_date : string
    parms_str : string

    Returns
    -------
    df : pandas.DataFrame

    """
    base_url = "https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py"

    identifier = "identifier=SinglePoint"
    user_community = "userCommunity=SSE"
    temporal_average = "tempAverage=DAILY"
    output_format = "outputList=JSON,ASCII"
    user = "user=anonymous"

    url = (
        f"{base_url}?request=execute&{identifier}&{parms_str}&"
        f"startDate={str_start_date}&endDate={str_end_date}&"
        f"lat={lat}&lon={lon}&{temporal_average}&{output_format}&"
        f"{user_community}&{user}"
    )
    response = requests.get(url)
    data_json = response.json()
    df = pd.DataFrame(data_json['features'][0]['properties']['parameter'])
    df['lon'] = lon
    df['lat'] = lat
    return df


def nasa_data_area(bbox, str_start_date, str_end_date, parms_list):
    """
    Extract data for an area. The area is at most 10x10 degrees, the output is
    at 1/2 degrees coordinates.

    Parameters
    ----------
    bbox : list
        [min lat, min lon, max lat, max lon], half-degrees
        max 10x10 degrees
    str_start_date : string
    str_end_date : string
    parms_list : list

    Returns
    -------
    df : pandas.DataFrame

    """
    base_url = "https://power.larc.nasa.gov/cgi-bin/v1/DataAccess.py"

    identifier = "identifier=Regional"
    parms_str = f"parameters={','.join(parms_list)}"
    user_community = "userCommunity=SSE"
    temporal_average = "tempAverage=DAILY"
    output_format = "outputList=JSON"
    user = "user=anonymous"

    url = (
        f"{base_url}?request=execute&{identifier}&{parms_str}&"
        f"startDate={str_start_date}&endDate={str_end_date}&"
        f"bbox={str(bbox)[1:-1].replace('. ', '').replace(' ', '')}&"
        f"{temporal_average}&{output_format}&"
        f"{user_community}&{user}"
    )
    print(bbox)

    response = requests.get(url).json()
    data_json = requests.get(response['outputs']['json']).json()
    data = [
        pd.DataFrame({**{par: data_coord['properties']['parameter'][par]
                         for par in parms_list},
                      'lat': data_coord['geometry']['coordinates'][1],
                      'lon': data_coord['geometry']['coordinates'][0]
                      }) for data_coord in data_json['features']
    ]
    df = pd.concat(data)
    df.reset_index(inplace=True, drop=False)
    return df.rename(columns={'index': 'date'})


def match_grid_point(locations, df_data):
    """
    Match data from the grid to the single locations.

    Parameters
    ----------
    locations : pd.DataFrame
        Unique locations.
    df_data : pd.DataFrame
        The grid data.

    Returns
    -------
    pd.DataFrame
        Output dataset.

    """
    data = []
    for row in locations.itertuples():
        lat = 0.5 * round(2 * (row.lat - 0.25)) + 0.25
        lon = 0.5 * round(2 * (row.lon - 0.25)) + 0.25
        df_loc = df_data[(df_data.lat == lat) & (df_data.lon == lon)].copy()
        df_loc.lat = row.lat
        df_loc.lon = row.lon

        data.append(df_loc)
    return pd.concat(data).reset_index(drop=True, inplace=False)


def nasa_connector(df_locations, start_date, end_date=None, parms=None,
                   precision='area'):
    """Retrieve meteorologic data from NASA.

    Given a dataset with columns country, region, sub_region, lon, and lat, for
    each geographic coordinate (lon, lat) corresponding to a place (specified
    if country, region, or sub_region) extract the time series of the desired
    data at the location.

    Arguments:
    ---------
        df_locations(pandas.DataFrame): Dataset with columns lon, and lat
        start_date(datetime): Start date for the time series
        end_date(datetime): End date for the time series (optional)
        parms(list of strings): Desired data, accepted are 'temperature',
                                'humidity', and 'pressure' (optional)
        precision(string): Either 'area' (deafault) for lower precision but
                           much faster running time, or 'point' for more
                           precise but much slower running time.

    Return:
    ------
        pandas.DataFrame:   Columns are country, region, sub_region (non-null),
                            lon, lat, date, and the desired data.
    """
    if parms is None:
        parms = list(PARAMETERS.keys())

    df_locations = df_locations[
        ~pd.isna(df_locations[['lon', 'lat']]).all(axis=1)
    ]
    location_data = ['country', 'region', 'sub_region', 'lon', 'lat']
    locations = df_locations[location_data].drop_duplicates()

    str_start_date = str(start_date.date()).replace('-', '')

    if end_date is None:
        str_end_date = str(pd.Timestamp.today().date()).replace('-', '')
    else:
        str_end_date = str(end_date.date()).replace('-', '')

    all_parms = list(itertools.chain.from_iterable([PARAMETERS[p] for p in parms]))
    parms_str = f"parameters={','.join(all_parms)}"

    if precision == 'point':
        return pd.concat([
            nasa_data_loc(row.lat, row.lon, str_start_date, str_end_date, parms_str)
            for row in locations.itertuples()
        ])
    else:
        df_data = pd.concat(
            [nasa_data_area(list(bbox), str_start_date,
                            str_end_date, all_parms)
             for bbox in area_partition(locations)]
        )
        df_data.reset_index(drop=True, inplace=True)
        return match_grid_point(locations, df_data)
