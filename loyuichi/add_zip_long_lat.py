from geopy.geocoders import GoogleV3
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

# Adding zip code field to all documents that do not have it yet
geolocator = GoogleV3()

for meter in repo['loyuichi.meters'].find({'X': {'$exists': True}, 'Y': {'$exists': True}}):
    try:
        location = geolocator.reverse(str(meter['Y']) + ',' + str(meter['X']))

        if (location):
            zipcode = location[0].raw['address_components'][-1]["long_name"]
            if (len(zipcode) == 4):
                    zipcode = "0" + zipcode
            res = repo['loyuichi.meters'].update({'_id': meter['_id']}, {'$set': {'zip': zipcode}})
            print(res)
    except:
        pass

for fe in repo['loyuichi.food_establishments'].find({'zip': {'$exists': False}}):
	try:
		location = geolocator.reverse(str(fe['location']['latitude']) + ', ' + str(fe['location']['longitude']))

		if (location):
			zipcode = location[0].raw['address_components'][-1]["long_name"]
			if (len(zipcode) == 4):
				zipcode = "0" + zipcode
			repo['loyuichi.food_establishments'].update({'_id': fe['_id']}, {'$set': {'zip': zipcode}})
	except:
		pass

for towed in repo['loyuichi.towed'].find():
	try:
		location = geolocator.reverse(str(towed['location']['latitude']) + ',' + str(towed['location']['longitude']))

		if (location):
			zipcode = location[0].raw['address_components'][-1]["long_name"]
			if (len(zipcode) == 4):
				zipcode = "0" + zipcode
			res = repo['loyuichi.towed'].update({'_id': towed['_id']}, {'$set': {'zip': zipcode}})
			print(res)
	except:
		pass

for ticket in repo['loyuichi.tickets'].find({"zip": {'$exists': False}}):
	try:
		street_add = ticket["ticket_loc"]
		
		location = geolocator.geocode(street_add + " Boston")

		zipcode = location.raw['address_components'][-1]["long_name"]
		# Ensure all zipcodes are 5-digit
		if (len(zipcode) == 4):
			zipcode = "0" + zipcode
		latitude = location.latitude
		longitude = location.longitude
		addresses[street_add] = {'zip': zipcode, 'location': {'latitude': latitude, 'longitude': longitude}}
		res = repo['loyuichi.tickets'].update({'_id': ticket['_id']}, {'$set': {'zip': zipcode, 'location': {'latitude': latitude, 'longitude': longitude}}})
		print(res)
	except:
		pass

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

this_script = doc.agent('alg:add_zip_long_lat', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

food_establishments = doc.entity('dat:food_establishments', {'prov:label':'Food Establishment Permits', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
meters = doc.entity('dat:meters', {'prov:label':'Parking Meters', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
towed = doc.entity('dat:towed', {'prov:label':'Towed', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
tickets = doc.entity('dat:tickets', {'prov:label':'Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

zip_meters = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Zip Meters', prov.model.PROV_TYPE:'ont:Computation'})
zip_food_establishments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Zip Food Establishments', prov.model.PROV_TYPE:'ont:Computation'})
zip_towed = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Zip Towed', prov.model.PROV_TYPE:'ont:Computation'})
zip_longlat_tickets = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Zip Longitude Latitude Tickets', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(zip_meters, this_script)
doc.wasAssociatedWith(zip_food_establishments, this_script)
doc.wasAssociatedWith(zip_towed, this_script)
doc.wasAssociatedWith(zip_longlat_tickets, this_script)

doc.used(zip_food_establishments, food_establishments, startTime)
doc.used(zip_meters, meters, startTime)
doc.used(zip_towed, towed, startTime)
doc.used(zip_longlat_tickets, tickets, startTime)

db_food_establishments = doc.entity('dat:food_establishments', {prov.model.PROV_LABEL:'Food Establishments Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_food_establishments, this_script)
doc.wasGeneratedBy(db_food_establishments, zip_food_establishments, endTime)
doc.wasDerivedFrom(db_food_establishments, food_establishments, zip_food_establishments, zip_food_establishments, zip_food_establishments)

db_meters = doc.entity('dat:meters', {prov.model.PROV_LABEL:'Parking Meters', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_meters, this_script)
doc.wasGeneratedBy(db_meters, zip_meters, endTime)
doc.wasDerivedFrom(db_meters, meters, zip_meters, zip_meters, zip_meters)

db_towed = doc.entity('dat:towed', {prov.model.PROV_LABEL:'Towed', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_towed, this_script)
doc.wasGeneratedBy(db_towed, zip_towed, endTime)
doc.wasDerivedFrom(db_towed, towed, zip_towed, zip_towed, zip_towed)

db_tickets = doc.entity('dat:tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(db_tickets, this_script)
doc.wasGeneratedBy(db_tickets, zip_longlat_tickets, endTime)
doc.wasDerivedFrom(db_tickets, tickets, zip_longlat_tickets, zip_longlat_tickets, zip_longlat_tickets)

#repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
#open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof


