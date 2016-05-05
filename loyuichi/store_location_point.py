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

# for meter in repo['loyuichi.meters'].find({'X': {'$exists': True}, 'Y': {'$exists': True}}):
#     x = meter['X']
#     y = meter['Y']
#     res = repo['loyuichi.meters'].update({'_id': meter['_id']}, {'$set': {'location': {'type': "Point", 'coordinates': [x, y]}}})
#     print(res)

# for restaurants in repo['loyuichi.food_establishments'].find({'location.longitude': {'$exists': True}, 'location.latitude': {'$exists': True}}):
#     x = float(restaurants['location']['longitude'])
#     y = float(restaurants['location']['latitude'])
#     res = repo['loyuichi.food_establishments'].update({'_id': restaurants['_id']}, {'$set': {'location_point': {'type': "Point", 'coordinates': [x, y]}}})
#     print(res)

# for ticket in repo['loyuichi.tickets'].find({'location.longitude': {'$exists': True}, 'location.latitude': {'$exists': True}}):
#     x = float(ticket['location']['longitude'])
#     y = float(ticket['location']['latitude'])
#     res = repo['loyuichi.tickets'].update({'_id': ticket['_id']}, {'$set': {'location_point': {'type': "Point", 'coordinates': [x, y]}}})
#     print(res)

# for tow in repo['loyuichi.towed'].find({'x': {'$exists': True}, 'y': {'$exists': True}}):
#     x = float(tow['x'])
#     y = float(tow['y'])
#     res = repo['loyuichi.towed'].update({'_id': tow['_id']}, {'$set': {'location_point': {'type': "Point", 'coordinates': [x, y]}}})
#     print(res)

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

this_script = doc.agent('alg:store_location_point', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

towed = doc.entity('dat:towed', {'prov:label':'Towed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Parking Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

update_towed_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Update Towed Location', prov.model.PROV_TYPE:'ont:Computation'})
update_meters_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Update Meters Location', prov.model.PROV_TYPE:'ont:Computation'})
update_tickets_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Update Tickets Location', prov.model.PROV_TYPE:'ont:Computation'})
update_food_establishments_location = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Update Food Establishments Location', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(update_towed_location, this_script)
doc.wasAssociatedWith(update_meters_location, this_script)
doc.wasAssociatedWith(update_tickets_location, this_script)
doc.wasAssociatedWith(update_food_establishments_location, this_script)

doc.used(update_towed_location, towed, startTime)
doc.used(update_food_establishments_location, food_establishments, startTime)
doc.used(update_tickets_location, tickets, startTime)
doc.used(update_meters_location, meters, startTime)

db_towed = doc.entity('dat:towed', {prov.model.PROV_LABEL:'Towed', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_towed, this_script)
doc.wasGeneratedBy(db_towed, update_towed_location, endTime)
doc.wasDerivedFrom(db_towed, towed, update_towed_location, update_towed_location, update_towed_location)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, update_food_establishments_location, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, update_food_establishments_location, update_food_establishments_location, update_food_establishments_location)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, update_tickets_location, endTime)
doc.wasDerivedFrom(db_tickets, tickets, update_tickets_location, update_tickets_location, update_tickets_location)

db_meters = doc.entity('dat:meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_meters, this_script)
doc.wasGeneratedBy(db_meters, update_meters_location, endTime)
doc.wasDerivedFrom(db_meters, meters, update_meters_location, update_meters_location, update_meters_location)
#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
