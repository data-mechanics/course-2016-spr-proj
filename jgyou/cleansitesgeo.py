# https://docs.mongodb.org/manual/reference/operator/projection/positional/
# https://docs.mongodb.org/manual/reference/operator/update/unset/
from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

# pass in collection name
def flatten(db, X, field):
	for x in db[X].find():
		db[X].aggregate([{'$unwind': '$' + field }])

def deletefield(db, X, Y):
	for x in db[X].find():
		for y in Y:
			db[X].update_one({}, {'$unset': {y}})


# set up repo

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = loads(f)
user = auth['user']

repo.authenticate(user, user)

startTime = datetime.datetime.now()

##########

'''
# ideally copy collection first, then collect
flatten(repo, user + ".sitegeocodes", "features")
deletefield(repo, user + ".sitegeocodes", ["properties", "type"])
flatten(repo, user + ".sitegeocodes", "geometry")
'''


repo.dropPermanent("sitecoordinates")
repo.createPermanent("sitecoordinates")

# loop through full geocode data to just pull out


# boston api data is organized y-x lat-long, so reverse coordinates accordingly
for code in repo[user + '.sitegeocodes'].find():
	siteid = code['siteid']
	[x, y] = code['features'][0]['geometry']['coordinates']
	repo[user + '.sitecoordinates'].insert({'siteid': siteid, 'coordinates': [x, y], 'latitude': y, 'longitude': x})


###########

endTime = datetime.datetime.now()

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script = provdoc.agent('alg:cleansitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('dat:sitegeocodes', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Extension'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime)

output = provdoc.entity('dat:sitecoordinates', {prov.model.PROV_LABEL:'X-Y Site Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(output, this_script)
provdoc.wasGeneratedBy(output, this_run, endTime)
provdoc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

repo.logout()



