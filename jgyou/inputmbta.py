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
import zipfile
import io


exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()

with open("auth.json") as a:
	f = a.read()

	auth = loads(f)
	user = auth['user']

	repo.authenticate(auth['user'], auth['user'])


	with request.urlopen("http://www.mbta.com/uploadedfiles/MBTA_GTFS.zip") as resp:
		response = resp.read()
		with zipfile.ZipFile(io.BytesIO(response)) as z:
			for file in z.namelist():
				if file in ['stops.txt']:
					with z.open(file,"r") as f2:
						mbtatextfile = f2.readlines()
						#mbtatext = request.urlopen("http://datamechanics.io/data/jgyou/stops.txt").read()

						mbtainfo = []

						for stop in mbtatextfile[1:]:
							#print(stop)
							allstr = stop.decode("utf-8").replace('"', "").split(",")
							stop_id = allstr[0]
							stop_name = allstr[2]
							try:
								stop_lat = float(allstr[4])
							except ValueError:
								print(allstr[4])
								stop_lat = "NA"
							try:
								stop_lon = float(allstr[5])	
							except ValueError:
								stop_lat = "NA"
							wheelchair = allstr[10]
							mbtainfo.append({"stop_id": stop_id, "stop_name": stop_name, \
								"longitude": stop_long, "latitude": stop_lat, "wheelchair": wheelchair})

						repo.dropPermanent("mbtaStops")
						repo.createPermanent("mbtaStops")


						repo[user + '.mbtaStops'].insert_many(mbtainfo)


						endTime = datetime.datetime.now()
	###########









