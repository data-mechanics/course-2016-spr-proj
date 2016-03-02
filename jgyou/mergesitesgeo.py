'''
mergesitesgeo.py

Merges cleaned coordinates with current drop off sites data set
'''

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


provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script = provdoc.agent('alg:mergesitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
source1 = provdoc.entity('dat:sitecoordinates', {prov.model.PROV_LABEL:'X-Y Site Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, source1, startTime)

source2 = provdoc.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, source2, startTime)
provdoc.wasAttributedTo(source2, this_script)
provdoc.wasGeneratedBy(source2, this_run, endTime)
provdoc.wasDerivedFrom(source2, source1, this_run, this_run, this_run)
provdoc.wasDerivedFrom(source2, source2, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.


repo.logout()