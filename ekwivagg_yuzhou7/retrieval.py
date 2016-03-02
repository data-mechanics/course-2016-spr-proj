import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
auth = open('auth.json', 'r')
cred = json.load(auth)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

repo.dropPermanent("found")
repo.dropPermanent("lost")
repo.dropPermanent("_provenance")
repo.dropPermanent("_registry")

url = 'https://data.cityofboston.gov/resource/gb6y-34cq.json?$select=businessname,location'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("restaurant")
repo.createPermanent("restaurant")
repo['ekwivagg_yuzhou7.restaurant'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/ciur-a7cc.json?type=Rodent+Activity&$select=geocoded_location'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("hotline")
repo.createPermanent("hotline")
repo['ekwivagg_yuzhou7.hotline'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/qndu-wx8w.json?$select=businessname,result,resultdttm,violdesc,violdttm,violstatus'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("inspection")
repo.createPermanent("inspection")
repo['ekwivagg_yuzhou7.inspection'].insert_many(r)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:Retrieval', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})



resource = doc.entity('bdp:gb6y-34cq', {'prov:label':'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=businessname,location'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

restaurant = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Restaurant', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(restaurant, this_script)
doc.wasGeneratedBy(restaurant, this_run, endTime)
doc.wasDerivedFrom(restaurant, resource, this_run, this_run, this_run)



resource = doc.entity('bdp:ciur-a7cc', {'prov:label':'Mayors 24 Hour Hotline', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=Rodent+Activity&$select=geocoded_location'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

rodent = doc.entity('dat:hotline', {prov.model.PROV_LABEL:'Rodent Activity', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(rodent, this_script)
doc.wasGeneratedBy(rodent, this_run, endTime)
doc.wasDerivedFrom(rodent, resource, this_run, this_run, this_run)



resource = doc.entity('bdp:qndu-wx8w', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=businessname,result,resultdttm,violdesc,violdttm,violstatus'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

inspection = doc.entity('dat:inspection', {prov.model.PROV_LABEL:'Restaurant Inspection', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(inspection, this_script)
doc.wasGeneratedBy(inspection, this_run, endTime)
doc.wasDerivedFrom(inspection, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
content = json.dumps(json.loads(doc.serialize()), indent=4)

f = open('plan.json', 'w')
f.write(content)
#print(doc.get_provn())
repo.logout()

## eof