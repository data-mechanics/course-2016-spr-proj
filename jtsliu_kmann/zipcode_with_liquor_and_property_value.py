import datetime
import json
import prov.model
import pymongo
import urllib.request
import uuid

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

repo.dropPermanent("zipcode_liquor_property")
repo.createPermanent("zipcode_liquor_property")

test = repo['crime']

for x in test:
	print (x)
	break;