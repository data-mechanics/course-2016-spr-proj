"""
File: distance_estimators.py

Description: A collection (to be) of different distance estimates using numpy
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""


import numpy as np


def spherical_distance(pos1, pos2, r=3958.75):
    """
    Calculates the distance between two np arrays with [Longitude, Latitude]

    Modified from http://stackoverflow.com/a/19414306

    Parameters
    ----------
    pos1: np.array
    pos2: np.array
    r: float

    Returns
    -------
    np.array
    """

    pos1 *= np.pi / 180
    pos2 *= np.pi / 180

    cos_lat1 = np.cos(pos1[..., 0])
    cos_lat2 = np.cos(pos2[..., 0])
    cos_lat_d = np.cos(pos1[..., 0] - pos2[..., 0])
    cos_lon_d = np.cos(pos1[..., 1] - pos2[..., 1])
    dist = r * np.arccos(cos_lat_d - cos_lat1 * cos_lat2 * (1 - cos_lon_d))
    return dist
