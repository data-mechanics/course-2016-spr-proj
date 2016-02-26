from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo


f = open("auth.json").read()

auth = loads(f)
user = auth['user']
# remember to modify this line later
repo.authenticate(auth['user'], auth['user'])

startTime = datetime.datetime.now()

##########

# beforehand, convert longitude and latitude fields to doubles

###########

endTime = datetime.datetime.now()

repo.logout()