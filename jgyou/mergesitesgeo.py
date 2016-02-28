from json import loads
import pymongo
import prov.model
import datetime
import uuid
from bson.objectid import ObjectId

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = loads(f)
user = auth['user']
repo.authenticate(user, user)


startTime = datetime.datetime.now()

##########

for coord in repo[user + '.sitecoordinates'].find():
	siteid = coord["siteid"] 
	c = coord["coordinates"]
	latitude = coord["latitude"]
	longitude = coord["latitude"]
	repo[user + '.currentsites'].update({"_id": ObjectId(siteid)}, {"$set": {"latitude": latitude, "longitude": longitude, "coordinates": c}})


###########

endTime = datetime.datetime.now()


# prov updates



repo.logout()