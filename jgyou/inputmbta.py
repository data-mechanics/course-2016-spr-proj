'''
inputmbta.py

saved textfile from mbta GTFS
https://www.mbta.com/uploadedfiles/MBTA_GTFS.zip

'''

from urllib import parse, request
from json import loads, dumps, load

import pymongo
import prov.model
import datetime
import uuid


client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()

f = open("auth.json").read()

auth = loads(f)
user = auth['user']

repo.authenticate(auth['user'], auth['user'])

mbtatext = request.urlopen("http://datamechanics.io/data/jgyou/stops.txt").read()

mbtainfo = []

for stop in mbtatext:
	allstr = stop.replace('"', "").split(",")
	stop_id = allstr[0]
	stop_name = allstr[2]
	stop_lat = float(allstr[4])
	stop_long = float(allstr[5])
	wheelchair = allstr[10]
	mbtainfo.append({"stop_id": stop_id, "stop_name": stop_name, \
		"longitude": stop_long, "latitude": stop_lat, "wheelchair": wheelchair})

repo.dropPermanent("mbtaStops")
repo.createPermanent("mbtaStops")


repo[user + '.mbtaStops'].insert_many(mbtainfo)


endTime = datetime.datetime.now()
###########3









