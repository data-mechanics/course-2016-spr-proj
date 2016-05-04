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

# for meter in repo['loyuichi.meters'].find({'X': {'$exists': True}, 'Y': {'$exists': True}}):
#     x = meter['X']
#     y = meter['Y']
#     res = repo['loyuichi.meters'].update({'_id': meter['_id']}, {'$set': {'location': {'type': "Point", 'coordinates': [x, y]}}})
#     print(res)

for restaurants in repo['loyuichi.food_establishments'].find({'location.longitude': {'$exists': True}, 'location.latitude': {'$exists': True}}):
    x = float(restaurants['location']['longitude'])
    y = float(restaurants['location']['latitude'])
    res = repo['loyuichi.food_establishments'].update({'_id': restaurants['_id']}, {'$set': {'location_point': {'type': "Point", 'coordinates': [x, y]}}})
    print(res)

