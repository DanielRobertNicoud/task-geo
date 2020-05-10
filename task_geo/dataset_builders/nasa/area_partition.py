import numpy as np


def area_partition(df_loc):
    """
    Find a small number of small bboxes covering all the locations.

    Parameters
    ----------
    df_loc : pandas.DataFrame
        Need to contain columns 'lat' and 'lon' with the coordinates.

    Returns
    -------
    numpy.Array
        Size is (number of boxes, 4).

    """

    # location points
    unique_locations = df_loc[['lat', 'lon']].drop_duplicates().dropna()

    # create new columns with top left corner of small bbox containing the
    # location
    unique_locations['bottom_left_lat'] = \
        np.floor(unique_locations.lat / 4.5) * 4.5
    unique_locations['bottom_left_lon'] = \
        np.floor(unique_locations.lon / 4.5) * 4.5
    unique_locations['top_right_lat'] = \
        unique_locations['bottom_left_lat'] + 4.5
    unique_locations['top_right_lon'] = \
        unique_locations['bottom_left_lon'] + 4.5

    bboxes = unique_locations[['bottom_left_lat',
                               'bottom_left_lon',
                               'top_right_lat',
                               'top_right_lon']]

    return bboxes.drop_duplicates().values
