import datetime
import math;
from .DangerZone import DangerZone

class Pothole(DangerZone):
    baseLevel = 10

    def __init__(self, lat, lng, createdAt, status):
        self.lat = lat
        self.lng = lng
        self.createdAt = datetime.datetime.strptime(createdAt, '%Y-%m-%dT%H:%M:%S.%f')
        self.closed = (status == 'Closed')

    def calculateTimeDanger(self):
        # How old is the pothole
        curTime = datetime.datetime.now()
        age = (curTime - self.createdAt).days

        return math.exp((age / 30) / 12)

    def calculateDangerLevel(self):
        # Pothole fixed, no danger
        if self.closed:
            return 0

        return self.baseLevel + self.calculateTimeDanger()

# p = Pothole(42.3594, -71.0587, '2014-03-27T13:53:08.000', '2014-03-27T15:47:12.000')
# print p.calculateDangerLevel();
