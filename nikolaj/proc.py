import urllib.request
import json
import pymongo
import datetime
import inspect

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('nikolaj', 'nikolaj')

earnings = repo.nikolaj.earnings_2014

for doc in earnings.find():
    print(doc)
