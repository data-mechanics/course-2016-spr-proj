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

url = "https://data.cityofboston.gov/resource/ufcx-3fdn.json"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)


s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("crime")
repo.createPermanent("crime")

# Filter out the bad data
for elem in r:
	if elem['location']['coordinates'] != [0, 0]:
		repo['jtsliu_kmann.crime'].insert_one(elem)

#repo['jtsliu_kmann.crime'].insert_many(r)

# print(json.dumps(json.loads(response), sort_keys=True, indent=2))
