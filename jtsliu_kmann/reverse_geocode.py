import datetime
import json
import prov.model
import pymongo
import urllib.request
import uuid
from geopy.geocoders import Nominatim # use this for getting zipcodes

geolocator = Nominatim()

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Parse through dataases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp


repo.dropPermanent("crime_data_with_zipcode")
repo.createPermanent("crime_data_with_zipcode")

crime_data = getCollection('crime')

count = 0
for x in crime_data:
	if count == 2000:
		break
	lng = x['location']['coordinates'][0]
	lat = x['location']['coordinates'][1]

	location = geolocator.reverse(str(lat) + ", " + str(lng))

	if not 'postcode' in location.raw['address']:
		continue
	zipcode = location.raw['address']['postcode']
	x['zipcode'] =  zipcode

	repo['jtsliu_kmann.crime_data_with_zipcode'].insert_one(x)
	count += 1

repo.createPermanent("crime_occurance_by_zipcode")

pipeline = [
	{ "$group" : {"_id": "$zipcode", "number_crimes": {"$sum": 1}}},
]

crimes_per_zip = list(repo['jtsliu_kmann.crime_data_with_zipcode'].aggregate(pipeline))
repo['jtsliu_kmann.crime_occurance_by_zipcode'].insert_many(crimes_per_zip)


