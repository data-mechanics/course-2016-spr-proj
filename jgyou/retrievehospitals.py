'''
retrievehospitals.py

This script retrieves list of hospitals from City of Boston website, 
and loads it into the repo.
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

app_token = auth['service']['cityofbostondataportal']['token']
query = "name, ad, location_zip, location, neigh"

urlbdp = " https://data.cityofboston.gov/resource/u6fv-m8v4.json?" + \
	"$select=" + parse.quote_plus(query) + "&$$app_token="  \
	+ app_token
	
responsebdp = request.urlopen(urlbdp).read().decode("utf-8")
r1 = json.loads(responsebdp)
repo.dropPermanent("hospitals")
repo.createPermanent("hospitals")
#s = json.dumps(r, sort_keys=True, indent=2)
repo[user + '.hospitals'].insert_many(r1)


endTime = datetime.datetime.now()

###############

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')		# city of boston data.


this_script = provdoc.agent('alg:retrievedata', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = provdoc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval', \
	'ont:Query':'?select=name%2C+ad%2C+location_zip%2C+location%2C+neigh'})

needle311 = provdoc.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(needle311, this_script)
provdoc.wasGeneratedBy(needle311, this_run, endTime)
provdoc.wasDerivedFrom(needle311, resource, this_run, this_run, this_run)

open('plan.json', 'w').write(json.dumps(json.loads(provdoc.serialize()), indent=4))
repo.record(provdoc.serialize()) # Record the provenance document.

#print(provdoc.get_provn())
repo.logout()
