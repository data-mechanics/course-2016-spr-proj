# ####################################################################
#
# CS 591 - Data Mechanics (Prof. Lapets)
#
# p1.py
# by Daren McCulley (djmcc) and Jasper Burns (jasper)
#

import urllib.request
import json
import pymongo
# import prov.model
import datetime
import uuid

# ############################### SETUP ################################

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('djmcc_jasper', 'djmcc_jasper')

startTime = datetime.datetime.now()

# ############################## GET DATA ###############################

# Retrieve crimes
url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?$select=location&' \
      '$limit=3000&$where=fromdate%20%3E=%20%272015-02-23%27%20AND%20fromdate%20%3C%20%272015-03-04%27'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)

repo.dropPermanent("all_crimes")
repo.createPermanent("all_crimes")
repo['djmcc_jasper.all_crimes'].insert_many(r)

# Retrieve 911 Reports
url = 'https://data.cityofboston.gov/resource/uea6-pfmm.json' \
      '?$select=latitude,longitude&$where=latitude%20IS%20NOT%20null&' \
      '$limit=20000'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)

repo.dropPermanent("emergency_calls")
repo.createPermanent("emergency_calls")
repo['djmcc_jasper.emergency_calls'].insert_many(r)

# ############################ ANALYZE DATA ###############################

# Independent variables (both at one minute)
latTolerance = 0.0167
longTolerance = 0.0167

# Helpers
allCrimes = repo.djmcc_jasper.all_crimes.find().sort([
    ("location.latitude", pymongo.DESCENDING),
    ("location.longitude", pymongo.DESCENDING)
])
emergencyCalls = repo.djmcc_jasper.emergency_calls.find().sort([
    ("latitude", pymongo.DESCENDING),
    ("longitude", pymongo.DESCENDING)
])
wasInRange = False


def in_range(crime, call):
    crime_lat = float(crime['location']['latitude'])
    crime_long = float(crime['location']['longitude'])
    call_lat = float(call['latitude'])
    call_long = float(call['longitude'])

    latDif = abs(crime_lat-call_lat)
    longDif = abs(crime_long-call_long)

    distSquared = latDif**2 + longDif**2

    # If either lat or long is outside tolerance, return false
    if (latDif > latTolerance) or (longDif > longTolerance):
        return None
    else:
        return distSquared

for crime in allCrimes:

    nearbyCalls = []
    currentClosestCall = [None,9999]

    for call in emergencyCalls:

        # If in range, add to list of nearby calls
        distSquared = in_range(crime, call)
        if distSquared is not None:

            if distSquared < currentClosestCall[1]:
                currentClosestCall = [call['_id'],distSquared]

            wasInRange = True
            nearbyCalls.append(call['_id'])

        # Else, if we were in lat range (but now out of range), then we're done. Break For loop. (only works if sorted)
        else:
            if wasInRange and (abs(float(crime['location']['latitude'])-float(call['latitude'])) > latTolerance):
                # throw away the crime, and the closest call (if one exists)
                if currentClosestCall[0] is not None:
                    repo.djmcc_jasper.all_crimes.delete_one({"_id":crime["_id"]})
                    repo.djmcc_jasper.emergency_calls.delete_one({"_id":currentClosestCall[0]})
                break

    # Add nearby calls to document
    crime.update({"nearbyCalls": nearbyCalls})

    # This adds to the real database
    # repo.djmcc_jasper.all_crimes.update_one({'_id': crime['_id']}, {"$set": {"nearbyCalls": nearbyCalls}})


# ############################### SHUTDOWN ################################

endTime = datetime.datetime.now()
repo.logout()

# eof