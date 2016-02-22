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

key = ""

repo.dropTemporary("sitecoordinates")
repo.createTemporary("sitecoordinates")


for site in repo['jgyou.currentsites'].find():	
	param = site['location_street_name'] + "," + site['neighborhood'] + ", MA" + " " + site['location_zipcode']

	query = "https://api.opencagedata.com/geocode/v1/json?q=" + parse.quote_plus(param) + "&limit=1" + "&pretty=1" + "&countrycode=us" +"&key=" + key
	georesult =  request.urlopen(query).read().decode("utf-8")
	repo['jgyou.sitecoordinates'].insert(loads(georesult))
	print(dumps(georesult))


###########

endTime = datetime.datetime.now()




