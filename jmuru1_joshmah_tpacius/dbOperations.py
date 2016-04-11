import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import apiTest as apiTest

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_joshmah_tpacius', 'jmuru1_joshmah_tpacius')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

#Josh Stuff ===========================================================
# repo.dropPermanent("streetjams")
# repo.createPermanent("streetjams")
# repo['jmuru1_joshmah_tpacius.streetjams'].insert_many(apiTest.streetJams)
# # x = repo['joshmah.streetjams'].find({});
#
# repo.dropPermanent("hospitals")
# repo.createPermanent("hospitals")
# repo['jmuru1_joshmah_tpacius.hospitals'].insert_many(apiTest.hospitals)
#
# repo.dropPermanent("emsdeparture")
# repo.createPermanent("emsdeparture")
# repo['jmuru1_joshmah_tpacius.emsdeparture'].insert_many(apiTest.emsDeparture)
#Josh Stuff end =============================================================

repo.dropPermanent("propertyvalue")
repo.createPermanent("propertyvalue")
repo['jmuru1_joshmah_tpacius.propertyvalue'].insert_many(apiTest.propertyvalue)
# ========================query database=================================


def getCollection(dbName):
	temp = []
	if type(dbName) != str:
		return "Error: please input a string"
	for elem in repo['jmuru1_joshmah_tpacius.' + dbName].find({}):
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
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_joshmah_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_joshmah_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:dbProvOps', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource3 = doc.entity('bdp:n7za-nsjh', {'prov:label':'Boston Property Values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$limit=1000'})
doc.wasAssociatedWith(this_run, this_script)

doc.used(this_run, resource3, startTime)


propertyvalues = doc.entity('dat:propertyvalues', {prov.model.PROV_LABEL:'Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(propertyvalues, this_script)
doc.wasGeneratedBy(propertyvalues, this_run, endTime)
doc.wasDerivedFrom(propertyvalues, resource3, this_run, this_run, this_run)

this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceHospitals = doc.entity('bdp:46f7-2snz', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceStreetjams = doc.entity('bdp:yqgx-2ktq', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_hospitals = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=ad&?$select=ad,name'})
get_streetjams = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=endnode'})
doc.wasAssociatedWith(get_hospitals, this_script)
doc.wasAssociatedWith(get_streetjams, this_script)
doc.used(get_streetjams, resourceStreetjams, startTime)
doc.used(get_hospitals, resourceHospitals, startTime)

hospitals = doc.entity('dat:hospitals', {prov.model.PROV_LABEL:'ad', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(hospitals, this_script)
doc.wasGeneratedBy(hospitals, get_hospitals, endTime)
doc.wasDerivedFrom(hospitals, resourceHospitals, get_hospitals, get_hospitals, get_hospitals)

streetjams = doc.entity('dat:streetjams', {prov.model.PROV_LABEL:'street', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(streetjams, this_script)
doc.wasGeneratedBy(streetjams, get_streetjams, endTime)
doc.wasDerivedFrom(streetjams, resourceStreetjams, get_streetjams, get_streetjams, get_streetjams)

# repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
