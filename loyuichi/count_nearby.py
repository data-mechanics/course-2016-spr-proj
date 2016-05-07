import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

meters_per_mile = 1609.34

res = repo['loyuichi.food_establishments'].drop_indexes()
print(res)
res = repo['loyuichi.food_establishments'].create_index([('location_point', pymongo.GEOSPHERE)], unique=False)
print(res)

res = repo['loyuichi.meters'].drop_indexes()
print(res)
res = repo['loyuichi.meters'].create_index([('location', pymongo.GEOSPHERE)], unique=False)
print(res)

res = repo['loyuichi.tickets'].drop_indexes()
print(res)
res = repo['loyuichi.tickets'].create_index([('location_point', pymongo.GEOSPHERE)], unique=False)
print(res)

# res = repo['loyuichi.towed'].drop_indexes()
# print(res)
# res = repo['loyuichi.towed'].create_index([('location_point', pymongo.GEOSPHERE)], unique=False)
# print(res)

data = []
repo.dropPermanent('fe_radius')
repo.createPermanent('fe_radius')
for fe in repo['loyuichi.food_establishments'].find():
	coordinates = fe['location_point']['coordinates']
	meters_count = repo['loyuichi.meters'].count({ 'location': { '$nearSphere': { '$geometry': { 'type': "Point", 'coordinates': coordinates }, '$maxDistance': 0.4 * meters_per_mile } } })
	tickets_count = repo['loyuichi.tickets'].count({ 'location_point': { '$nearSphere': { '$geometry': { 'type': "Point", 'coordinates': coordinates }, '$maxDistance': 0.4 * meters_per_mile } } })
	# towed_count = repo['loyuichi.towed'].count({ 'location_point': { '$nearSphere': { '$geometry': { 'type': "Point", 'coordinates': coordinates }, '$maxDistance': 0.7 * meters_per_mile } } })
	score = meters_count + (-0.3*tickets_count)
	data += [{'_id': fe['_id'], 'name': fe['businessname'], 'location_point': fe['location_point'], 'meters': meters_count, 'tickets': tickets_count, 'score': score}]
repo['loyuichi.fe_radius'].insert_many(data)


#Outputting the results to a JSON file formatted for heatmap.html
data = []
for fe in repo['loyuichi.fe_radius'].find({}, {'_id': 0}):
	print(fe)
	data += [fe]

with open('fe_radius.json', 'w') as outfile:
	out = "var food_establishments = "
	out += json.dumps(data)
	outfile.write(out)

data = []
for m in repo['loyuichi.meters'].find({}, {'_id': 0, 'location': 1}):
	data += [m]

with open('meters.json', 'w') as outfile:
	out = "var meters = "
	out += json.dumps(data)
	outfile.write(out)



