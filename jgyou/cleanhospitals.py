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

with open("auth.json") as f:
	auth = json.loads(f.read())
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
		repo[user + '.hospitals'].update({"_id": h["_id"]}, {"$set": {"longitude": lon, "latitude": lat, "location_type": "hospital"}})


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
	provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
	provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
	provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
	provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


	this_script2 = provdoc2.agent('alg:cleanhospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
	resource2 = provdoc2.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
	this_run2 = provdoc2.activity('log:a'+run_id, None, None, {prov.model.PROV_TYPE:'ont:Computation'})
	provdoc2.wasAssociatedWith(this_run2, this_script2)
	provdoc2.used(this_run2, resource2, None)

	output2 = resource2
	provdoc2.wasAttributedTo(output2, this_script2)
	provdoc2.wasGeneratedBy(output2, this_run2, None)
	provdoc2.wasDerivedFrom(output2, resource2, this_run2, this_run2, this_run2)

	with open('plan.json','r') as plan:
		docModel = prov.model.ProvDocument()
		doc2 = docModel.deserialize(plan)
		doc2.update(provdoc2)
		plan.close()
		with open('plan.json', 'w') as plan:
			plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
			plan.close()


		#repo.record(provdoc.serialize()) # Record the provenance document.

		#print(provdoc.get_provn())
			repo.logout()
