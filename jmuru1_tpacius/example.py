import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import apitest as apitest
import geo as geo

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_tpacius', 'jmuru1_tpacius')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

#The collections are being created and populated here

# repo.dropPermanent("zipcarmembers")
# repo.createPermanent("zipcarmembers")
# repo['jmuru1_tpacius.zipcarmembers'].insert_many(apitest.zipCarMemberCount)

# repo.dropPermanent("zipcarreservations")
# repo.createPermanent("zipcarreservations")
# repo['jmuru1_tpacius.zipcarreservations'].insert_many(apitest.zipCarReservations)

# repo.dropPermanent("tickets")
# repo.createPermanent("tickets")
# repo['jmuru1_tpacius.tickets'].insert_many(geo.rawAddr)

# ========================query databse=================================
#ticketsCollection = repo['jmuru1_tpacius.tickets'].find({})
#print(ticketsCollection)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

lost = doc.entity('dat:lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(lost, this_script)
doc.wasGeneratedBy(lost, this_run, endTime)
doc.wasDerivedFrom(lost, resource, this_run, this_run, this_run)

found = doc.entity('dat:found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(found, this_script)
doc.wasGeneratedBy(found, this_run, endTime)
doc.wasDerivedFrom(found, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
