import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Active_Food_Establishment_Licenses.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("foodEst")
repo.createPermanent("foodEst")
repo['jlam17_mckay678.foodEst'].insert_many(r)

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Corner Stores.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("cornerStore")
repo.createPermanent("cornerStore")
repo['jlam17_mckay678.cornerStore'].insert_many(r)

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Food Pantries.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("pantries")
repo.createPermanent("pantries")
repo['jlam17_mckay678.pantries'].insert_many(r)

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Retail Bakery.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("bakeries")
repo.createPermanent("bakeries")
repo['jlam17_mckay678.bakeries'].insert_many(r)

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Summer Farmers\' Markets.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("summerFM")
repo.createPermanent("summerFM")
repo['jlam17_mckay678.summerFM'].insert_many(r)

url = 'https://github.com/ktmck678/course-2016-spr-proj-one/tree/master/Data/Winter Farmers Markets.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("winterFM")
repo.createPermanent("winterFM")
repo['jlam17_mckay678.winterFM'].insert_many(r)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/alice_bob/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/alice_bob/') # The data sets in <user>/<collection> format.
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
