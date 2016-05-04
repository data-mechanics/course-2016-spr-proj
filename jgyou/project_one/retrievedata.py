'''
retrievedata.py

This script retrieves a subset of data from the City of Boston 
website related to their Needle Program, and loads it into the repo.
'''

from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time

exec(open('../pymongo_dm.py').read())

# set up connection

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

# retrieve City of Boston data on needle program

startTime = datetime.datetime.now()

query = "Needle Pickup"

app_token = auth['service']['cityofbostondataportal']['token']

urlbdp = "https://data.cityofboston.gov/resource/wc8w-nujj.json?type=" + parse.quote_plus(query)  \
	+ "&$select=longitude,latitude,location,geocoded_location,open_dt" + "&$$app_token="  \
	+ app_token
	
responsebdp = request.urlopen(urlbdp).read().decode("utf-8")
r1 = json.loads(responsebdp)
repo.dropPermanent("needle311")
repo.createPermanent("needle311")
#s = json.dumps(r, sort_keys=True, indent=2)
repo[user + '.needle311'].insert_many(r1)

endTime = datetime.datetime.now()


# record provenance data

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')		# city of boston data.

run_id = str(uuid.uuid4())

this_script = provdoc.agent('alg:retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval', \
	'ont:Query':'?type=Needle+Pickup&$select=longitude,latitude,location,geocoded_location,open_dt'})

needle311 = provdoc.entity('dat:needle311', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(needle311, this_script)
provdoc.wasGeneratedBy(needle311, this_run, endTime)
provdoc.wasDerivedFrom(needle311, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.


# record for plan.json without timestamps
provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc2.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')		# city of boston data.

this_script = provdoc2.agent('alg:retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc2.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = provdoc2.activity('log:a'+run_id)
provdoc2.wasAssociatedWith(this_run, this_script)
provdoc2.used(this_run, resource, None, None, {prov.model.PROV_TYPE:'ont:Retrieval', \
	'ont:Query':'?type=Needle+Pickup&$select=longitude,latitude,location,geocoded_location,open_dt'})

needle311 = provdoc2.entity('dat:needle311', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc2.wasAttributedTo(needle311, this_script)
provdoc2.wasGeneratedBy(needle311, this_run)
provdoc2.wasDerivedFrom(needle311, resource, this_run, this_run, this_run)


open('plan.json','w').write(json.dumps(json.loads(provdoc2.serialize()), indent=4))
repo.logout()



