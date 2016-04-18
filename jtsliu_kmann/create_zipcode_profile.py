# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# 
import datetime
import json
import prov.model
import pymongo
import re
import urllib.request
import uuid
from bson.son import SON

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
# Josh Mah helped us with this one :)
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

startTime = datetime.datetime.now()

repo.dropPermanent('jtsliu_kmann.zipcode_profile')
repo.createPermanent('jtsliu_kmann.zipcode_profile')

count_crimes_per_zip_aggregation = [
	{ "$group" : {"_id": "$zip", "num_crimes": {"$sum": 1}}}
]

count_schools_per_zip_aggregation = [
	{ "$group" : {"_id": "$zipcode", "num_schools": {"$sum": 1}}}
]

count_hospitals_per_zip_aggregation = [
	{ "$group" : {"_id": "$zipcode", "num_hospitals": {"$sum": 1}}}
]

crimes_per_zip = list(repo['jtsliu_kmann.liquor_influenced_crime'].aggregate(count_crimes_per_zip_aggregation))
schools_per_zip = list(repo['jtsliu_kmann.public_schools'].aggregate(count_schools_per_zip_aggregation))
hospitals_per_zip = list(repo['jtsliu_kmann.hospitals'].aggregate(count_hospitals_per_zip_aggregation))

zipcode_info = getCollection("zipcode_liquor_property_info")

# Build up the profiles
profiles = []
for zipcode in zipcode_info:
	# get the crime number
	for zip_crime in crimes_per_zip:
		if zipcode["_id"] == zip_crime["_id"]:
			zipcode["num_crimes"] = zip_crime["num_crimes"]
			break
	if not "num_crimes" in zipcode:
		zipcode["num_crimes"] = 0

	# get the hospitals
	for zip_hospitals in hospitals_per_zip:
		if zipcode["_id"] == zip_hospitals["_id"]:
			zipcode["num_hospitals"] = zip_hospitals["num_hospitals"]
	if not "num_hospitals" in zipcode:
		zipcode["num_hospitals"] = 0

	# get the schools
	for zip_schools in schools_per_zip:
		if zipcode["_id"] == zip_schools["_id"]:
			zipcode["num_schools"] = zip_schools["num_schools"]
	if not "num_schools" in zipcode:
		zipcode["num_schools"] = 0

	# additional clean up
	if not "liquor_locations" in zipcode:
		zipcode["liquor_locations"] = 0

	profiles.append(zipcode)

repo['jtsliu_kmann.zipcode_profile'].insert_many(profiles)

endTime = datetime.datetime.now()

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:create_zipcode_profile', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

resource1 = doc.entity('dat:zipcode_liquor_property_info',
	{prov.model.PROV_LABEL:'Zipcode Liquor and Propery Data', prov.model.PROV_TYPE:'ont:DataSet'}
)

resource2 = doc.entity('dat:liquor_influenced_crime',
	{prov.model.PROV_LABEL:'Crime near Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'}
)

resource3 = doc.entity('dat:public_schools',
	{prov.model.PROV_LABEL:'Public Schools', prov.model.PROV_TYPE:'ont:DataSet'}
)

resource4 = doc.entity('dat:hospitals',
	{prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'}
)

this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Computation'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)
doc.used(this_run, resource3, startTime)
doc.used(this_run, resource4, startTime)

zipcode_profile_data = doc.entity('dat:zipcode_profile', {prov.model.PROV_LABEL:'Zipcode Profile', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zipcode_profile_data, this_script)
doc.wasGeneratedBy(zipcode_profile_data, this_run, endTime)
doc.wasDerivedFrom(zipcode_profile_data, resource1, this_run, this_run, this_run)
doc.wasDerivedFrom(zipcode_profile_data, resource2, this_run, this_run, this_run)
doc.wasDerivedFrom(zipcode_profile_data, resource3, this_run, this_run, this_run)
doc.wasDerivedFrom(zipcode_profile_data, resource4, this_run, this_run, this_run)

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

