# https://docs.mongodb.org/manual/reference/operator/projection/positional/
# https://docs.mongodb.org/manual/reference/operator/update/unset/
from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

# remember to modify this line later
repo.authenticate("jgyou", "jgyou")

startTime = datetime.datetime.now()

##########

for coord in repo['sitegeocodes'].find():
	repo['sitegeocodes'].update({}, $unset: {'confidence'})

# note long-lat = x-y
# http://stackoverflow.com/questions/6851933/how-do-i-remove-a-field-completely-from-mongo

# then retrieve just long-lat data, store in site coordinates

###########

endTime = datetime.datetime.now()

repo.logout()

