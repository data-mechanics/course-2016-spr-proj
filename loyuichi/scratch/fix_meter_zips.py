import urllib.request
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
for meter in repo['loyuichi.meters'].find({"zip": {'$exists': True}, '$where': "this.zip.length == 4"}):
    zipcode = "0" + meter['zip']
    res = repo['loyuichi.meters'].update({'_id': meter['_id']}, {'$set': {'zip': zipcode}})
    print(res)