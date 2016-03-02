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

repo.dropPermanent("property_assessment")
repo.createPermanent("property_assessment")

count = 50000
iteration = 0
while count == 50000:
	url = "https://data.cityofboston.gov/resource/qz7u-kb7x.json?$limit=50000&$offset=" + str(50000 * iteration)
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	iteration += 1
	count = len(r)
	print("added", len(r), "records")
	repo['jtsliu_kmann.property_assessment'].insert_many(r)


