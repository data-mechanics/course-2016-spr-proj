import datetime
import math;
from .DangerZone import DangerZone

class Construction(DangerZone):
    baseLevel = 25

    def __init__(self, lat, lng, closedAt, sidewalkPlates, roadwayPlates):
        self.lat = lat
        self.lng = lng
        self.closedAt = datetime.datetime.strptime(closedAt, '%b %d %Y %I:%M%p')
        self.sidewalkPlates = sidewalkPlates
        self.roadwayPlates = roadwayPlates

    def calculateSidewalkDanger(self):
        if self.sidewalkPlates:
            return int(self.sidewalkPlates)
        else:
            return 0;

    def calculateRoadwayDanger(self):
        if self.roadwayPlates:
            return int(self.roadwayPlates)
        else:
            return 0;

    def calculateTimeDanger(self):
        curTime = datetime.datetime.now()
        timeLeft = (self.closedAt - curTime).days

        # Construction is finished
        if timeLeft < 0:
            return 0

        return 0

    def calculateDangerLevel(self):
        time = self.calculateTimeDanger()
        sidewalk = self.calculateSidewalkDanger()
        roadway = self.calculateRoadwayDanger()

        # Construction is finished, no danger
        # if time == 0:
        #     return 0
        # else:
        #     return self.baseLevel + time + sidewalk + roadway

        return self.baseLevel + time + sidewalk + roadway

# c = Construction(42.3594, -71.0587, '2016-03-27T13:53:08.000', 5, 12)
# print c.calculateDangerLevel();
