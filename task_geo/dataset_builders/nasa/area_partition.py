import numpy as np
from numpy.linalg import norm
from sklearn.cluster import KMeans


def area_partition(df_loc):
    """
    Find a small number of small bboxes covering all the locations.

    Using k-means repeatedly, we find a small number of boxes of side at most
    10 covering all the given geolocations.

    Notes:
        - Does not consider -180 as close to 180. This can lead to suboptimal
        solutions (but nothing too bad).
        - The fit is doen with the Euclidean distance, not with the
        L-infinity metric (which would fit squares). This leads to slightly
        suboptimal solutions.

    Parameters
    ----------
    df_loc : pandas.DataFrame
        Need to contain columns 'lat' and 'lon' with the coordinates.

    Returns
    -------
    numpy.Array
        Size is (number of boxes, 4).

    """

    # points to cluster
    unique_locations = df_loc[['lat', 'lon']].drop_duplicates().dropna().values

    # do k-means with increasing k until the maximal radius is no bigger than 5
    k = 0
    while True:
        k += 1
        kmeans = KMeans(n_clusters=k).fit(unique_locations)

        cluster_centers = kmeans.cluster_centers_
        labels = kmeans.labels_
        cluster_radii = np.empty(k)
        for i in range(k):
            cluster_radii[i] = max([norm(el - cluster_centers[i])
                                    for el in unique_locations[labels == i, :]]
                                   )
        max_radius = cluster_radii.max()

        # if the radius is small enough, create bboxes and return
        if max_radius <= 5:
            bboxes = np.empty((k, 4))
            for i in range(k):
                cx, cy = cluster_centers[i, :]
                r = cluster_radii[i]
                bboxes[i] = [0.5 * np.floor(2 * (cx - r)),
                             0.5 * np.floor(2 * (cy - r)),
                             0.5 * np.ceil(2 * (cx + r)),
                             0.5 * np.ceil(2 * (cy + r))]
                # widen slightly bboxes with zero area
                if bboxes[i, 0] == bboxes[i, 2]:
                    bboxes[i, 0] -= 0.5
                    bboxes[i, 2] += 0.5
                if bboxes[i, 1] == bboxes[i, 3]:
                    bboxes[i, 1] -= 0.5
                    bboxes[i, 3] += 0.5
            return bboxes
