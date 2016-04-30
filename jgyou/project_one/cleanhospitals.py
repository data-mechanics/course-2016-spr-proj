'''
cleanhospitals.py

A script that has nothing to do with hospital hygiene
but has everything to do with updating field names,
incorporating fields, etc. to make data more amenable
to use later.

Parse apart address info and rename fields as needed
location_street_name
location
latitude
longitude


Rename
neighborhood
resource_name
neighborhood
location_zipcode
'''

from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

# set up connection

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

startTime = datetime.datetime.now()


#############

# rename fields
repo[user + '.hospitals'].update({},{"$rename": {'ad': 'location_street_name', 'name': 'resource_name', 'neigh': 'neighborhood', 'location_zip': 'location_zipcode'}})

# update other fields
for h in repo[user + '.hospitals'].find():
	lon = h["location"]["coordinates"][0]
	lat = h["location"]["coordinates"][1]
	repo[user + '.hospitals'].update({"_id": h["_id"]}, {"longitude": lon, "latitude": lat, "location_type": "hospital"})


endTime = datetime.datetime.now()

###############

run_id = str(uuid.uuid4())

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script = provdoc.agent('alg:cleanhospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+run_id, startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime)

output = resource
provdoc.wasAttributedTo(output, this_script)
provdoc.wasGeneratedBy(output, this_run, endTime)
provdoc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

######
# plan

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


this_script = provdoc.agent('alg:cleanhospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+run_id, startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime)

output = resource
provdoc.wasAttributedTo(output, this_script)
provdoc.wasGeneratedBy(output, this_run, endTime)
provdoc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

#print(provdoc.get_provn())
repo.logout()
