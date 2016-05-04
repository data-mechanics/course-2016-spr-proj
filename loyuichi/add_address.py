from geopy.geocoders import Nominatim
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def street_abbrev(address):
	street_suffixes = {"Road": "Rd", "Street": "St", "Avenue": "Av", "Place": "Pl"}
	for word in street_suffixes:
		if (word in address):
			return address.replace(word, street_suffixes[word])

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('loyuichi', 'loyuichi')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# Standardizing street name abbreviations
geolocator = Nominatim()

#for meter in repo['loyuichi.meters'].find({'streetname': {'$exists': False}}):
	# try:
	# 	location = geolocator.reverse(str(meter['Y']) + ', ' + str(meter['X']))

	# 	if ('road' in location.raw['address']):
	# 		street_address = street_abbrev(location.raw['address']['road'])
	# 		repo['loyuichi.meters'].update({'_id': meter['_id']}, {'$set': {'streetname': street_address}})
	# except:
	# 	pass

# for fe in repo['loyuichi.food_establishments'].find({'streetname': {'$exists': False}}):
# 	try:
# 		location = geolocator.reverse(str(fe['location']['latitude']) + ', ' + str(fe['location']['longitude']))

# 		if ('road' in location.raw['address']):
# 			street_address = street_abbrev(location.raw['address']['road'])
# 			repo['loyuichi.food_establishments'].update({'_id': fe['_id']}, {'$set': {'streetname': street_address}})
# 	except:
# 		pass
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

this_script = doc.agent('alg:add_address', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Parking Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

address_meters = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Address Meters', prov.model.PROV_TYPE:'ont:Computation'})
address_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Address Food Establishments', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(address_meters, this_script)
doc.wasAssociatedWith(address_food_establishments, this_script)

doc.used(address_food_establishments, food_establishments, startTime)
doc.used(address_meters, meters, startTime)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, address_food_establishments, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, address_food_establishments, address_food_establishments, address_food_establishments)

db_meters = doc.entity('dat:meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_meters, this_script)
doc.wasGeneratedBy(db_meters, address_meters, endTime)
doc.wasDerivedFrom(db_meters, meters, address_meters, address_meters, address_meters)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof


