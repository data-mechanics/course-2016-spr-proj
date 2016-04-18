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

exec(open('../pymongo_dm.py').read())


def makeProvZipCodes(repo, run_id, starttime, endtime):
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    provdoc.add_namespace('madir', 'http://www.directoryma.com/')        # MA Directory
    #provdoc.add_namespace('aws', 'https://data-mechanics.s3.amazonaws.com/')   # Amazon Web Services
     

    this_script = provdoc.agent('dat:' + 'jgyou', {prov.model.PROV_TYPE:prov.model.PROV['Person']})
    resource = provdoc.entity('madir:MAReferenceDesk/MassachusettsZipCodes', {'prov:label':'MA Zip Codes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'html'})
    this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
    provdoc.wasAssociatedWith(this_run, this_script)

    provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    zipcodes = provdoc.entity('dat:zipcodes', {prov.model.PROV_LABEL:'Boston Zipcodes', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    provdoc.wasAttributedTo(zipcodes, this_script)
    provdoc.wasGeneratedBy(zipcodes, this_run, endTime)
    provdoc.wasDerivedFrom(zipcodes, resource, this_run, this_run, this_run)

    if starttime == None:
        plan = open('plan.json','r')
        docModel = prov.model.ProvDocument()
        doc = docModel.deserialize(plan)
        doc.update(provdoc)
        plan.close()
        plan = open('plan.json', 'w')
        plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
        plan.close()
    else:
        repo.record(provdoc.serialize()) 


## for senior centsrs/community centers
def makeProvCommunity(repo, run_id, starttime, endtime):
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    provdoc.add_namespace('elderly', 'www.cityofboston.gov/elderly/')


    this_script = provdoc.agent('dat:' + 'jgyou', {prov.model.PROV_TYPE:prov.model.PROV['Person']})
    resource = provdoc.entity('elderly:agency', {'prov:label':'Agencies on Aging Grantee Programs', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'asp'})
    resource2 = provdoc.entity('elderly:center', {'prov:label':'Veronica B. Smith Senior Center', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'asp'})
    this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
    provdoc.wasAssociatedWith(this_run, this_script)

    provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    centers = provdoc.entity('dat:seniorservices', {prov.model.PROV_LABEL:'Senior Services', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    provdoc.wasAttributedTo(centers, this_script)
    provdoc.wasGeneratedBy(centers, this_run, endTime)
    provdoc.wasDerivedFrom(centers, resource, this_run, this_run, this_run)

    if starttime == None:
        plan = open('plan.json','r')
        docModel = prov.model.ProvDocument()
        doc = docModel.deserialize(plan)
        doc.update(provdoc)
        plan.close()
        plan = open('plan.json', 'w')
        plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
        plan.close()
    else:
        repo.record(provdoc.serialize()) 



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

run_id_zipcodes = str(uuid.uuid4())
runids_communitycenter = str(uuid.uuid4())

makeProvZipCodes(repo, run_id_zipcodes, startTime, endTime)

makeProvZipCodes(repo, run_id_zipcodes, None, None)
makeProvCommunity(repo, runids_communitycenter, startTime, endTime)
makeProvCommunity(repo, runids_communitycenter, None, None)

#print(provdoc.get_provn())
repo.logout()
