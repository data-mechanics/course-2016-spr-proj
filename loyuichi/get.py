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
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?INCIDENT_TYPE_DESCRIPTION=towed'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("towed")
repo.createPermanent("towed")
repo['loyuichi.towed'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/gb6y-34cq.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("food_establishments")
repo.createPermanent("food_establishments")
repo['loyuichi.food_establishments'].insert_many(r)

url = 'https://data.cityofboston.gov/resource/qbxx-ev3s.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("tickets")
repo.createPermanent("tickets")
repo['loyuichi.tickets'].insert_many(r)

url = 'https://data-mechanics.s3.amazonaws.com/loyuichi/parking_meters.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("meters")
repo.createPermanent("meters")
repo['loyuichi.meters'].insert_many(r)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/loyuichi/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/loyuichi/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:get', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

towed = doc.entity('dat:towed', {'prov:label':'Towed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Parking Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_towed = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
get_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
get_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
get_meters = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)

doc.wasAssociatedWith(get_towed, this_script)
doc.wasAssociatedWith(get_food_establishments, this_script)
doc.wasAssociatedWith(get_tickets, this_script)
doc.wasAssociatedWith(get_meters, this_script)

doc.used(get_towed, towed, startTime, None,
        {prov.model.PROV_TYPE:'ont:Retrieval',
         'ont:Query':'?INCIDENT_TYPE_DESCRIPTION=towed'
        }
    )
doc.used(get_food_establishments, food_establishments, startTime)
doc.used(get_tickets, tickets, startTime)
doc.used(get_meters, meters, startTime)

db_towed = doc.entity('dat:towed', {prov.model.PROV_LABEL:'Towed', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_towed, this_script)
doc.wasGeneratedBy(db_towed, get_towed, endTime)
doc.wasDerivedFrom(db_towed, towed, get_towed, get_towed, get_towed)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, get_food_establishments, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, get_food_establishments, get_food_establishments, get_food_establishments)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, get_tickets, endTime)
doc.wasDerivedFrom(db_tickets, tickets, get_tickets, get_tickets, get_tickets)

db_meters = doc.entity('dat:meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_meters, this_script)
doc.wasGeneratedBy(db_meters, get_meters, endTime)
doc.wasDerivedFrom(db_meters, meters, get_meters, get_meters, get_meters)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
