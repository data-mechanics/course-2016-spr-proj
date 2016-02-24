import urllib.request
import json
import pymongo
import datetime

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('nikolaj', 'nikolaj')

url = 'https://data.cityofboston.gov/resource/ntv7-hwjm.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)

startTime = datetime.datetime.now()

repo.createPermanent("earnings_2014")
repo['nikolaj.earnings_2014'].insert_many(r)
