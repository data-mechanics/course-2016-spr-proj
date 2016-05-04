'''
generatemiscprov.py

generates prov documentation for certain resources
that were not retrieved via scripts
'''

from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator

exec(open('../pymongo_dm.py').read())

# set up connection

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

####################################

startTime = datetime.datetime.now()


endTime = datetime.datetime.now()


###############

run_id = str(uuid.uuid4())

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('madir', 'http://www.directoryma.com/')		# MA Directory
provdoc.add_namespace('aws', 'https://data-mechanics.s3.amazonaws.com/')   # Amazon Web Services
 

this_script = provdoc.agent('dat:' + 'jgyou', {prov.model.PROV_TYPE:prov.model.PROV['Person']})
resource = provdoc.entity('madir:MAReferenceDesk/MassachusettsZipCodes', {'prov:label':'MA Zip Codes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'html'})
this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
provdoc.wasAssociatedWith(this_run, this_script)

provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

zipcodes = provdoc.entity('aws:jgyou/zipcodes', {prov.model.PROV_LABEL:'Boston Zipcodes', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
provdoc.wasAttributedTo(zipcodes, this_script)
provdoc.wasGeneratedBy(zipcodes, this_run, endTime)
provdoc.wasDerivedFrom(zipcodes, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

print(provdoc.get_provn())

##########
provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc2.add_namespace('madir', 'http://www.directoryma.com/')		# MA Directory
provdoc2.add_namespace('aws', 'https://data-mechanics.s3.amazonaws.com/')   # Amazon Web Services
 

this_script2 = provdoc2.agent('dat:' + 'jgyou', {prov.model.PROV_TYPE:prov.model.PROV['Person']})
resource2 = provdoc2.entity('madir:MAReferenceDesk/MassachusettsZipCodes', {'prov:label':'MA Zip Codes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'html'})
this_run2 = provdoc2.activity('log:a'+run_id, startTime, endTime)
provdoc2.wasAssociatedWith(this_run2, this_script2)

provdoc2.used(this_run2, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

zipcodes2 = provdoc2.entity('aws:jgyou/zipcodes', {prov.model.PROV_LABEL:'Boston Zipcodes', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
provdoc2.wasAttributedTo(zipcodes2, this_script2)
provdoc2.wasGeneratedBy(zipcodes2, this_run2, endTime)
provdoc2.wasDerivedFrom(zipcodes2, resource2, this_run2, this_run2, this_run2)

plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)
doc2.update(provdoc2)
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
plan.close()


#print(provdoc.get_provn())
repo.logout()
