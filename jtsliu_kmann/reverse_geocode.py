# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script generates crime data with zipcodes and the count of crimes
# within a specific zipcode
import datetime
import json
import prov.model
import pymongo
import urllib.request
import uuid
from geopy.geocoders import Nominatim # use this for getting zipcodes

geolocator = Nominatim()

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Parse through dataases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

startTime = datetime.datetime.now()

# NOTE: currently do not re run this as we do not have requests for our API
# as a result, you would drop the data and be unable to reobtain it!
# repo.dropPermanent("crime_data_with_zipcode")
# repo.createPermanent("crime_data_with_zipcode")

crime_data = getCollection('crime')

count = 0
for x in crime_data:
	if count == 2000:
		break
	lng = x['location']['coordinates'][0]
	lat = x['location']['coordinates'][1]

	location = geolocator.reverse(str(lat) + ", " + str(lng))

	if not 'postcode' in location.raw['address']:
		continue
	zipcode = location.raw['address']['postcode']
	x['zipcode'] =  zipcode

	repo['jtsliu_kmann.crime_data_with_zipcode'].insert_one(x)
	count += 1

repo.createPermanent("crime_occurance_by_zipcode")

pipeline = [
	{ "$group" : {"_id": "$zipcode", "number_crimes": {"$sum": 1}}},
]

crimes_per_zip = list(repo['jtsliu_kmann.crime_data_with_zipcode'].aggregate(pipeline))
repo['jtsliu_kmann.crime_occurance_by_zipcode'].insert_many(crimes_per_zip)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:reverse_geocode', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crime Incidents', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Computation'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

# We created 2 new data sets, and now we have 2 new entities
crime_data_with_zipcode = doc.entity('dat:crime_data_with_zipcode', {prov.model.PROV_LABEL: 'Crime Data with Zipcodes', prov.model.PROV_TYPE:'ont:DataSet'})
zipcode_with_num_crimes = doc.entity('dat:crime_occurance_by_zipcode', {prov.model.PROV_LABEL: 'Zipcodes with Number of Crimes', prov.model.PROV_TYPE:'ont:DataSet'})

doc.wasAttributedTo(crime_data_with_zipcode, this_script)
doc.wasAttributedTo(zipcode_with_num_crimes, this_script)

doc.wasGeneratedBy(crime_data_with_zipcode, this_run, endTime)
doc.wasGeneratedBy(zipcode_with_num_crimes, this_run, endTime)

doc.wasDerivedFrom(crime_data_with_zipcode, resource, this_run, this_run, this_run)
doc.wasDerivedFrom(zipcode_with_num_crimes, resource, this_run, this_run, this_run)

repo.record(doc.serialize())

# Open the plan.json file and update it
plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)

doc2.update(doc)
plan.close()

plan = open('plan.json', 'w')

plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))

print(doc.get_provn())

plan.close()

repo.logout()

