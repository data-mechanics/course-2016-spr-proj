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

run_id = str(uuid.uuid4())

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script = provdoc.agent('alg:mergesitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
source1 = provdoc.entity('dat:sitecoordinates', {prov.model.PROV_LABEL:'X-Y Site Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+ run_id, startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
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

#########

# plan update

provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script2 = provdoc2.agent('alg:mergesitesgeo', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
source1 = provdoc2.entity('dat:sitecoordinates', {prov.model.PROV_LABEL:'X-Y Site Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc2.activity('log:a'+run_id, None, None, {prov.model.PROV_TYPE:'ont:Computation'})
provdoc2.wasAssociatedWith(this_run, this_script2)
provdoc2.used(this_run, source1)

source2 = provdoc2.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc2.wasAssociatedWith(this_run, this_script2)
provdoc2.used(this_run, source2)
provdoc2.wasAttributedTo(source2, this_script2)
provdoc2.wasGeneratedBy(source2, this_run)
provdoc2.wasDerivedFrom(source2, source1, this_run, this_run, this_run)
provdoc2.wasDerivedFrom(source2, source2, this_run, this_run, this_run)

plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)
doc2.update(provdoc2)
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
plan.close()


repo.logout()