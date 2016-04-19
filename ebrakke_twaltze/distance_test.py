import numpy as np

def distance(start, end, point):
    """ Return the distance from a point to a line in meters """
    lat1, lng1 = start
    lat2, lng2 = end
    lat3, lng3 = point
    R = 6371000.0

    y = np.sin(lng3 - lng1) * np.cos(lat3)
    x = np.cos(lat1) * np.sin(lat3) - np.sin(lat1) * np.cos(lat3) * np.cos(lat3 - lat1)
    bearing1 = np.rad2deg(np.arctan2(y,x))
    bearing1 = 360 - (bearing1 - 360 % 360)

    y2 = np.sin(lng2 - lng1) * np.cos(lat2)
    x2 = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(lat2 - lat1)
    bearing2 = np.rad2deg(np.arctan2(y2,x2))
    bearing2 = 360 - (bearing2 - 360 % 360)

    lat1Rad = np.deg2rad(lat1)
    lat3Rad = np.deg2rad(lat3)
    dLong = np.deg2rad(lng3 - lng1)

    distance13 = np.arccos(np.sin(lat1Rad) * np.sin(lat3Rad) + np.cos(lat1Rad)*np.cos(lat3Rad)*np.cos(dLong)) * R
    min_distance = np.fabs(np.arcsin(np.sin(distance13/R) * np.sin(np.deg2rad(bearing1) - np.deg2rad(bearing2))) * R)

    return min_distance
