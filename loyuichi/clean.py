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

# Renaming the streetnam field to streetname
repo['loyuichi.tickets'].update_many({}, {'$rename': {'street_nam': 'streetname'}})
repo['loyuichi.food_establishments'].update_many({}, {'$rename': {'street_nam': 'streetname'}})
# Standardizing street name abbreviations
streetnames = repo['loyuichi.tickets'].distinct('streetname') + repo['loyuichi.food_establishments'].distinct('streetname') 

for streetname in streetnames:
	if (" AVE" in streetname):
		repo['loyuichi.tickets'].update_many({'streetname': streetname}, {'$set': {'streetname': streetname.replace(" AVE", " AV")}})
		repo['loyuichi.food_establishments'].update_many({'streetname': streetname}, {'$set': {'streetname': streetname.replace(" AVE", " AV")}})
	elif (" SQUARE" in streetname):
		repo['loyuichi.tickets'].update_many({'streetname': streetname}, {'$set': {'streetname': streetname.replace(" SQUARE", " SQ")}})
		repo['loyuichi.food_establishments'].update_many({'streetname': streetname}, {'$set': {'streetname': streetname.replace(" SQUARE", " SQ")}})

# Removing Parking Ticket rows irrelevant to patron parking
not_violations = [
	"B - BUS STOP/STAND",
	"OVER POST LIM-ZONE B",
	"EXPIRED INSPECTION",
	"EXPIRED/NO PLATE",
	"HP-DV PLATE",
	"OVER 1FT FROM CURB"
]

delete_non_violations = repo['loyuichi.tickets'].delete_many({'violations1': {'$in': not_violations}})
print(delete_non_violations.deleted_count)
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

this_script = doc.agent('alg:clean', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

clean_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
clean_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)

doc.wasAssociatedWith(clean_food_establishments, this_script)
doc.wasAssociatedWith(clean_tickets, this_script)

doc.used(clean_food_establishments, food_establishments, startTime)
doc.used(clean_tickets, tickets, startTime)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, clean_food_establishments, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, clean_food_establishments, clean_food_establishments, clean_food_establishments)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, clean_tickets, endTime)
doc.wasDerivedFrom(db_tickets, tickets, clean_tickets, clean_tickets, clean_tickets)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
