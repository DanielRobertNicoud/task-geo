"""
Obtain longitude/latitude from location name.

Reference: https://nominatim.org/release-docs/develop/api/Search/
Source: OpenStreetMap
"""
import requests
import pandas as pd
import numpy as np


def osm_connector(**kwargs):
    """
    Return the geolocation for the given address.

    Search for location in OpenStreetMap. Location can either be given as
    country=..., region=...(optional), sub_region=...(optional),
    city=...(optional)
    or
    address=(...)

    Only return the most relevant result (throws an error if no results are
    found).

    Parameters
    ----------
    **kwargs : See below

    Keyword Arguments
    -----------------
    country : string
    region : string
    sub_region : string
    city : string

    or

    address : string
        Single query for which to search.

    Returns
    -------
    dict
        Raw data from OSM
    """
    if 'address' not in kwargs.keys():
        assert 'country' in kwargs.keys(), "Country needs to be given."
        if 'sub_region' in kwargs.keys():
            assert 'region' in kwargs.keys(), "If sub_region is given, " \
                "region needs to be given as well"
        if 'city' in kwargs.keys():
            assert 'sub_region' in kwargs.keys(), "If city is given, " \
                "sub_region needs to be given as well"
    else:
        assert 'address' in kwargs.keys(), "No valid parameter given: " \
            f"{kwargs}"

    base_url = ("https://nominatim.openstreetmap.org/search?"
                "format=json&limit=1")

    if 'address' in kwargs.keys():
        addr = f"q={kwargs['address']}"
    else:
        kw = ['country', 'region', 'sub_region', 'city']
        kw_map = {
            'country': 'country',
            'region': 'state',
            'sub_region': 'county',
            'city': 'city'
            }

        addr = "&".join([f"{kw_map[k]}={kwargs[k]}"
                         for k in kw if k in kwargs.keys()])

    url = f"{base_url}&{addr}"

    response = requests.get(url)
    return response.json()[0]


def osm_formatter(raw):
    """
    Format raw data from OSM.

    Parameters
    ----------
    raw : dict
        Raw data from OSM.

    Returns
    -------
    data : dict
        Formatted data containing:
            lon     Longitude
            lat     Latitude
            bbox    Bounding box
    """
    return {'lon': raw['lon'],
            'lat': raw['lat'],
            'bbox': raw['boundingbox']}


def osm_geolocation(**kwargs):
    """
    Return the geolocation for the given address.

    Search for location in OpenStreetMap. Location can either be given as
    country=..., region=...(optional), sub_region=...(optional),
    city=...(optional)
    or
    address=(...)

    Only return the most relevant result (throws an error if no results are
    found).

    Parameters
    ----------
    **kwargs : See below

    Keyword Arguments
    -----------------
    country : string
    region : string
    sub_region : string
    city : string

    or

    address : string
        Single query for which to search.

    Returns
    -------
    dict
        Raw data from OSM
    """
    return osm_formatter(osm_connector(**kwargs))


def osm_apply(row):
    """
    Retrieve geolocation from a row of an appropriate dataset.

    Auxiliary function.

    Parameters
    ----------
    row : named tuple
        Row of a dataset containing columns country, region, sub_region,
        and city.

    Returns
    -------
    dict
        Dictionary containing longitude, latitude, and bounding box of the
        location indicated by the row.

    """
    try:
        if ~pd.isna(row.city):
            gl = osm_geolocation(country=row.country, region=row.region,
                                 sub_region=row.sub_region, city=row.city)
        elif ~pd.isna(row.sub_region):
            gl = osm_geolocation(country=row.country, region=row.region,
                                 sub_region=row.sub_region)
        elif ~pd.isna(row.region):
            gl = osm_geolocation(country=row.country, region=row.region)
        else:
            gl = osm_geolocation(country=row.country)
    except IndexError:
        try:
            if ~pd.isna(row.city):
                addr = (f"{row.city},{row.sub_region},{row.region},"
                        f"{row.country}")
            elif ~pd.isna(row.sub_region):
                addr = f"{row.sub_region},{row.region},{row.country}"
            elif ~pd.isna(row.region):
                addr = f"{row.region},{row.country}"
            else:
                addr = f"{row.country}"
            gl = osm_geolocation(address=addr)
        except IndexError:
            gl = {'lon': np.nan, 'lat': np.nan, 'bbox': np.nan}
    return pd.Series(gl)


def osm(df):
    """
    Enrich dataset with geolocations.

    Parameters
    ----------
    df : pandas.DataFrame
        Needs to have columns country, region, and subregion.

    Returns
    -------
    pandas.DataFrame
        Same dataframe but with added columns containing longitude, latitude,
        and bounding box for the location indicated by each row.

    """
    df_out = df.copy()
    df_out[['lon', 'lat', 'bbox']] = df_out.apply(osm_apply, axis=1)
    return df_out
