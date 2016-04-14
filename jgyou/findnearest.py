import pymongo
import prov.model
import datetime
import uuid
import time

from urllib import request, parse
import json
from geopy.distance import vincenty

# calls mapquest to get walking distance for two lat-long coordinates
def findDistance(startLocation, endLocation):
	(startlat, startlon) = startLocation
	(endlat, endlon) = endLocation

	with open("auth.json") as authfile:
		auth = json.loads(authfile.read())
		key = auth["service"]["mapquest"]["key"]
	

		# query = "http://www.mapquestapi.com/directions/v2/route?key=" + \
		# 	key + "&from=" + str(startlat) + "," + str(startlon) + "&to=" + str(endlat) + "," + str(endlon) \
		# 	+ "&outFormat=json" + "&routeType=pedestrian"

		query = "http://www.mapquestapi.com/directions/v2/route?key=" + \
		 	key + "&from=" + str(startlat) + "," + str(startlon) + "&to=" + str(endlat) + "," + str(endlon) \
		 	+ "&outFormat=json" + "&routeType=pedestrian" + "&doReverseGeocode=false"

		response = request.urlopen(query).read().decode("utf-8")
		dist = json.loads(response)["route"]["distance"]

		return dist


# given a series of locations in a collection and single start location
# find the nearest location via walking distance
def findClosestCoordinate(repo, collec, startLocation):
	cursor = repo[collec].find()
	alldist = []
	for document in cursor:
		endLocation = (document["latitude"], document["longitude"])

		dist = findDistance(startLocation, endLocation)

		alldist.append([endLocation, dist])

	alldist2 = [b for [a, b] in alldist]
	mindist = min(alldist2)

	return mindist

# for a given collection of sites and a start location,
# find if distance between site and start is less than
# some upper bound
def boundedRadiusMBTA(repo, collec, startLocation, bound):
	cursor = repo[collec].find()
	object_ids = []
	for document in cursor:
		(endlat, endlon) = (document["latitude"], document["longitude"])
		endLocation = (endlat, endlon)
		dist = vincenty(startLocation, endLocation).miles
		if dist <= bound:
			object_ids.append((document["_id"], dist, endlat, endlon, document["stop_name"], document["wheelchair"]))

	return object_ids



#print(findDistance((42.3604, -71.0580),  (42.3600, -71.0562)))




