import datetime
import json
import pymongo
import urllib.request

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = "https://data.cityofboston.gov/resource/qz7u-kb7x.json"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("property_assessment")
repo.createPermanent("property_assessment")
repo['jtsliu_kmann.property_assessment'].insert_many(r)
