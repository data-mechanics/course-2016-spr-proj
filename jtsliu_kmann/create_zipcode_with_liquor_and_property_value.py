import datetime
import json
import prov.model
import pymongo
import urllib.request
import uuid
from bson.son import SON

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

repo.dropPermanent("zipcode_liquor_property_info")
repo.createPermanent("zipcode_liquor_property_info")

repo.createTemporary("zipcode_liquor_count")
repo.createTemporary("zipcode_tax_count")

liquor = getCollection('liquor_license')
prop = getCollection('property_assessment')

pipeline1 = [
	{ "$group" : {"_id": "$zip", "liquor_locations": {"$sum": 1}}}
]

zipcode_liquor_locations = list(repo['jtsliu_kmann.liquor_license'].aggregate(pipeline1))



# for license in liquor:
# 	zipcode = license["zip"]
# 	entry = {"zipcode" : zipcode, "x" : 1}
# 	repo['jtsliu_kmann.zipcode_liquor_count'].insert_one(entry)

for x in prop:
	# print(x['zipcode'])
	if not 'gross_tax' in x:
		continue
	if not 'zipcode' in x:
		continue
	if not 'land_sf' in x:
		continue
	sf = int(x['land_sf'])
	zipcode = x['zipcode']
	tax = int(x['gross_tax'])

	# cant calculate price per sf if its 0...
	if sf == 0:
		continue

	tax_per_sf = tax / sf
	entry = {"zipcode" : zipcode, "gross_tax" : tax, "y" : 1, "tax_per_sf" : tax_per_sf}
	repo['jtsliu_kmann.zipcode_tax_count'].insert_one(entry)

pipeline2 = [
	{ "$group" : {"_id": "$zipcode", "tax_per_sf_sum" : {"$sum": "$tax_per_sf"}, "number_properties": {"$sum": 1}}},
	{ "$project" : {"avg_tax_per_sf": {"$divide": ["$tax_per_sf_sum", "$number_properties"]}, "number_properties": 1} }
]

zipcode_avg_prop_tax = list(repo['jtsliu_kmann.zipcode_tax_count'].aggregate(pipeline2))

for location in zipcode_avg_prop_tax:
	desired = None
	for other in zipcode_liquor_locations:
		if location["_id"] == other["_id"]:
			desired = other
			break
	if not desired is None:
		location["liquor_locations"] = desired["liquor_locations"]

repo['jtsliu_kmann.zipcode_liquor_property_info'].insert_many(zipcode_avg_prop_tax)

