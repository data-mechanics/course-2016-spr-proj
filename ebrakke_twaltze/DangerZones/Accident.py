import datetime
import math;
from .DangerZone import DangerZone

class Accident(DangerZone):
    baseLevel = 50

    def __init__(self, lat, lng, createdAt):
        self.lat = lat
        self.lng = lng
        self.createdAt = datetime.datetime.strptime(createdAt, '%Y-%m-%dT%H:%M:%S.%f')

    def calculateDangerLevel(self):
        return self.baseLevel

# a = Accident(42.3594, -71.0587, '2015-07-16T16:50:00.000')
# print a.calculateDangerLevel();
