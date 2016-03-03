import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import time

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('tbeaudry', 'tbeaudry')

startTime = datetime.datetime.now()

url = 'https://data.cityofboston.gov/resource/23yb-cufe.json?'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("PS_LOC")
repo.createPermanent("PS_LOC")
repo['tbeaudry.PS_LOC'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/46f7-2snz.json?'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("HS_LOC")
repo.createPermanent("HS_LOC")
repo['tbeaudry.HS_LOC'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/uea6-pfmm.json?$limit=50000&$select=latitude,longitude,cad_event_type,start_standard_time,udo_event_location_full_street_address'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("P_911")
repo.createPermanent("P_911")
repo['tbeaudry.P_911'].insert_many(r)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/tbeaudry/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/tbeaudry/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:gather', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource_ps = doc.entity('bdp:23yb-cufe', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource_hs = doc.entity('bdp:46f7-2snz', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resource_911 = doc.entity('bdp:uea6-pfmm', {'prov:label':'Boston Police Department 911', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_ps = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?'})
get_hs = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?'})
get_911 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$limit=50000&$select=latitude,longitude,cad_event_type,start_standard_time,udo_event_location_full_street_address'})

doc.used(get_ps, resource_ps, startTime)
doc.used(get_hs, resource_hs, startTime)
doc.used(get_911, resource_911, startTime)

doc.wasAssociatedWith(get_ps, this_script)
doc.wasAssociatedWith(get_hs, this_script)
doc.wasAssociatedWith(get_911, this_script)




PS_LOC = doc.entity('dat:PS_LOC', {prov.model.PROV_LABEL:'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(PS_LOC, this_script)
doc.wasGeneratedBy(PS_LOC, get_ps, endTime)
doc.wasDerivedFrom(PS_LOC, resource_ps, get_ps, get_ps, get_ps)

HS_LOC = doc.entity('dat:HS_LOC', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(HS_LOC, this_script)
doc.wasGeneratedBy(HS_LOC, get_hs, endTime)
doc.wasDerivedFrom(HS_LOC, resource_hs, get_hs, get_hs, get_hs)

P_911  = doc.entity('dat:P_911', {prov.model.PROV_LABEL:'Boston Police Department 911', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(P_911, this_script)
doc.wasGeneratedBy(P_911, get_hs, endTime)
doc.wasDerivedFrom(P_911, resource_hs, get_hs, get_hs, get_hs)

repo.record(doc.serialize()) # Record the provenance document.
##print(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof








