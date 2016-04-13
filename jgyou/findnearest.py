import pymongo
import prov.model
import datetime
import uuid
import time

from urllib import request, parse
import json
from geopy.distance import vincenty

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


def findClosestCoordinate(repo, collec, startLocation):
	cursor = repo[collec].find()
	alldist = []
	for document in cursor:
		endLocation = (document["latitude"], document["longitude"])

		dist = findClosestCoordinate(startLocation, endLocation)

		alldist.append([endLocation, dist])

	m = min([b for [a, b] in alldist])

	return dist

client = pymongo.MongoClient()
repo = client.repo


print(findDistance((42.3604, -71.0580),  (42.3600, -71.0562)))




