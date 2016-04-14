from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid
from geopy.geocoders import Nominatim

#from geopy.geocoders import Nominatim

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

##########


startTime = datetime.datetime.now()

f = open("auth.json").read()

auth = loads(f)
user = auth['user']
# remember to modify this line later
repo.authenticate(auth['user'], auth['user'])


servicequery = "http://datamechanics.io/data/jgyou/seniorservices.json"

returned = request.urlopen(query).read().decode("utf-8")
services = json.loads(returned)

repo.dropPermanent("servicecenters")
repo.createPermanent("servicecenters")

repo[user + "servicecenters"].insert_many(services)

for jsite in repo[user + "servicecenters"].find():
	s = site['Street'] + "," + site['City'] + ", MA" + " " + site['Zipcode']
	param.append(s)

	query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(s) + "&limit=1" +\
	 "&pretty=1" + "&countrycode=us" +"&key=" + key
	georesult =  request.urlopen(query).read().decode("utf-8")
	geojs = json.loads(georesult)

	# tie together site id with coordinate data by adding a field to json
	geojs['siteid'] = str(site['_id'])
	geojs['latitude'] = float(geojs['features'][0]['geometry']['coordinates'][0])
	geojs['longitude'] = float(geojs['features'][0]['geometry']['coordinates'][1])
	georesult = dumps(geojs)

	# then insert coordinates into new collection
	repo[user + '.servicecenters'].insert(loads(georesult))

repo.logout()




