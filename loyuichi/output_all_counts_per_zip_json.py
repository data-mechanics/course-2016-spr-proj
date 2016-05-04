import json
import pymongo
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# Aggregate food establishment counts by zip code
fe = repo['loyuichi.food_establishments'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
fe_zips = repo['loyuichi.food_establishments'].distinct("zip")

# Aggregate ticket counts by zip code
tickets = repo['loyuichi.tickets'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
tickets_zips = repo['loyuichi.tickets'].distinct("zip")

# Aggregate ticket counts by zip code
towed = repo['loyuichi.towed'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
towed_zips = repo['loyuichi.towed'].distinct("zip")

# Aggregate ticket counts by zip code
meters = repo['loyuichi.meters'].aggregate([{'$group': {'_id': "$zip", 'count': { '$sum': 1 }}}])
meters_zips = repo['loyuichi.meters'].distinct("zip")

# Converting cursor objects to dictionaries
tickets_by_zip = {}
for ticket in tickets:
	tickets_by_zip.update({ticket["_id"]: ticket["count"]})

towed_by_zip = {}
for t in towed:
	towed_by_zip.update({t["_id"]: t["count"]})

fe_by_zip = {}
for f in fe:
	fe_by_zip.update({f["_id"]: f["count"]})
print(fe_by_zip)

meters_by_zip = {}
for m in meters:
	meters_by_zip.update({m["_id"]: m["count"]})
print(meters_by_zip)

data = []

# Create (zipcode, # of Food Establishment, # of Car Towings, # of Tickets, # of Meters) tuples based on zip codes
if (len(fe_zips) > len(towed_zips)):
	data = [(fe_zip, fe_by_zip[fe_zip], towed_by_zip[fe_zip], tickets_by_zip[fe_zip], meters_by_zip[fe_zip]) for fe_zip in fe_zips if fe_zip in towed_zips and fe_zip in meters_zips and fe_zip in tickets_zips]
else:
	data = [(towed_zip, fe_by_zip[towed_zip], towed_by_zip[towed_zip], tickets_by_zip[towed_zip], meters_by_zip[towed_zip]) for towed_zip in towed_zips if towed_zip in fe_zips and towed_zip in meters_zips and towed_zip in tickets_zips]

# Outputting the results to a JSON file formatted for scatter_plots.html
print(data)
with open('all_counts_per_zip.txt', 'w') as outfile:
	out = ""
	out += json.dumps(data)
	outfile.write(out)
endTime = datetime.datetime.now()