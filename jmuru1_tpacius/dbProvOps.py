import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import apitest as apitest

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_tpacius', 'jmuru1_tpacius')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

#The collections are being created and populated here

repo.dropPermanent("zipcarmembers")
repo.createPermanent("zipcarmembers")
repo['jmuru1_tpacius.zipcarmembers'].insert_many(apitest.zipCarMemberCount)

repo.dropPermanent("zipcarreservations")
repo.createPermanent("zipcarreservations")
repo['jmuru1_tpacius.zipcarreservations'].insert_many(apitest.zipCarReservations)

repo.dropPermanent("propertyvalue")
repo.createPermanent("propertyvalue")
repo['jmuru1_tpacius.propertyvalue'].insert_many(apitest.propertyvalue)
# ========================query database=================================

def getCollection(dbName):
	temp = []
	if type(dbName) != str:
		return "Error: please input a string"
	for elem in repo['jmuru1_tpacius.' + dbName].find({}):
		temp.append(elem)
	return temp


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

this_script = doc.agent('alg:dbProvOps', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('bdp:78f5-5i4e', {'prov:label':'Zipcar Member Counts By Zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource2 = doc.entity('bdp:498g-jbmi', {'prov:label':'Zipcar Boston Reservations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource3 = doc.entity('bdp:n7za-nsjh', {'prov:label':'Boston Property Values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$limit=1000'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)
doc.used(this_run, resource3, startTime)

zipcarmembers = doc.entity('dat:zipcarmembers', {prov.model.PROV_LABEL:'Zipcar Member Count', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zipcarmembers, this_script)
doc.wasGeneratedBy(zipcarmembers, this_run, endTime)
doc.wasDerivedFrom(zipcarmembers, resource1, this_run, this_run, this_run)

zipcarreservations = doc.entity('dat:zipcarreservations', {prov.model.PROV_LABEL:'Zipcar Reservations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zipcarreservations, this_script)
doc.wasGeneratedBy(zipcarreservations, this_run, endTime)
doc.wasDerivedFrom(zipcarreservations, resource2, this_run, this_run, this_run)

propertyvalues = doc.entity('dat:propertyvalues', {prov.model.PROV_LABEL:'Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(propertyvalues, this_script)
doc.wasGeneratedBy(propertyvalues, this_run, endTime)
doc.wasDerivedFrom(propertyvalues, resource3, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof