'''
retrievedata.py

This script takes street addresses from the drop-off sites data
to find geographic data from Opencage Data. 
(This site uses geocoding to convert addresses to longitude and latitude).
'''

from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

# remember to modify this line later
repo.authenticate("jgyou", "jgyou")

startTime = datetime.datetime.now()

##########

# authorization key
key = ""

repo.dropPermanent("sitegeocodes")
repo.createPermanent("sitegeocodes")


for site in repo['jgyou.currentsites'].find():	
	param = site['location_street_name'] + "," + site['neighborhood'] + ", MA" + " " + site['location_zipcode']

	query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(param) + "&limit=1" + "&pretty=1" + "&countrycode=us" +"&key=" + key
	georesult =  request.urlopen(query).read().decode("utf-8")
	repo['jgyou.sitegeocodes'].insert(loads(georesult))


###########

endTime = datetime.datetime.now()

repo.logout()




