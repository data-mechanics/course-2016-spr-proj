"""
File: 

Description: Calculates centroids for crime committed in Boston using k-means
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""


import numpy as np
import pandas
from sklearn import cluster

from mjclawar_rarshad.tools import pandas_funcs
from mjclawar_rarshad.tools.database_helpers import DatabaseHelper


def spherical_dist(pos1, pos2, r=3958.75):

    pos1 *= np.pi / 180
    pos2 *= np.pi / 180

    cos_lat1 = np.cos(pos1[..., 0])
    cos_lat2 = np.cos(pos2[..., 0])
    cos_lat_d = np.cos(pos1[..., 0] - pos2[..., 0])
    cos_lon_d = np.cos(pos1[..., 1] - pos2[..., 1])
    dist = r * np.arccos(cos_lat_d - cos_lat1 * cos_lat2 * (1 - cos_lon_d))
    return dist


def sse_group(df, columns):
    """
    Calculates the SSE for a cluster given a dataframe with a list of X columns
    Parameters
    ----------
    df: pandas.DataFrame
    columns: list

    Returns
    -------
    np.float
    """
    distances = spherical_dist(df[columns].values, np.array([df[columns].mean().values]))
    distances *= distances
    sse = sum(distances)
    return sse


def kmeans_crime(database_helper):
    assert isinstance(database_helper, DatabaseHelper)

    df = pandas_funcs.read_mongo_collection('mjclawar_rarshad.crime_incidents', database_helper,
                                            cols=['location'])
    print(df.head())

    df = df.merge(df['location'].apply(lambda x: pandas.Series({'NEEDS_RECODE': x['needs_recoding'],
                                                                'LONGITUDE': float(x['longitude']),
                                                                'LATITUDE': float(x['latitude'])})),
                  left_index=True, right_index=True)

    k_range = range(2, 15)

    df = df[(df['LONGITUDE'] != 0) & (df['LATITUDE'] != 0)].copy()
    n = len(df)
    bic_last = -np.inf

    preds = None
    centers = None

    for k in k_range:
        clust = cluster.KMeans(n_clusters=k, random_state=1)
        preds = clust.fit_predict(df[['LONGITUDE', 'LATITUDE']])
        centers = clust.cluster_centers_
        sse = df[['LONGITUDE', 'LATITUDE']].groupby(preds).apply(sse_group, ['LONGITUDE', 'LATITUDE']).sum()
        bic = n * np.log(sse / n) + k * np.log(n)
        pct_improve = (bic - bic_last) / bic_last
        if pct_improve < .01:
            break
        bic_last = bic

    df['GROUP'] = preds
    df = df[['LONGITUDE', 'LATITUDE', 'GROUP']]
    database_helper.insert_permanent_pandas('crime_centers', df)
