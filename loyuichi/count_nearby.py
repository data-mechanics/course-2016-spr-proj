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

res = repo['loyuichi.food_establishments'].create_index([('location_point', pymongo.GEOSPHERE)], unique=False)
print(res)
res = repo['loyuichi.food_establishments'].find({ 'location_point': { '$nearSphere': { '$geometry': { 'type': "Point", 'coordinates': [-71.10298856778745, 42.34718777836574 ] }, '$maxDistance': 0.5 * meters_per_mile } } }).count()
print(res)