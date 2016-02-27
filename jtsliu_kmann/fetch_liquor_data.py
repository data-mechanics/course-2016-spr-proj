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

url = "https://data.cityofboston.gov/resource/hda6-fnsh.json"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("liquor_license")
repo.createPermanent("liquor_license")

for elem in r:
	if not 'location' in elem:
		# Apparently some of the data is missing this field
		continue
	# They are stored as strings which is super cool
	if float(elem['location']['latitude']) != 0 or float(elem['location']['longitude']) != 0:
		repo['jtsliu_kmann.liquor_license'].insert_one(elem)

#repo['jtsliu_kmann.liquor_license'].insert_many(r)

