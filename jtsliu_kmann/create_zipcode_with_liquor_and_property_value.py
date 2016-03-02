# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script generates zipcode data that contains the average tax dollar per square foot
# of a zipcode in addition to the number of liquor selling locations in that zipcode
import datetime
import json
import prov.model
import pymongo
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

# Start the process
startTime = datetime.datetime.now()

repo.dropPermanent("zipcode_liquor_property_info")
repo.createPermanent("zipcode_liquor_property_info")

# Temporary collections for computations
repo.createTemporary("zipcode_liquor_count")
repo.createTemporary("zipcode_tax_count")

# load collections into lists
liquor = getCollection('liquor_license')
prop = getCollection('property_assessment')

# Aggregate all the locations of the liquor licenses by zipcode
pipeline1 = [
	{ "$group" : {"_id": "$zip", "liquor_locations": {"$sum": 1}}}
]

zipcode_liquor_locations = list(repo['jtsliu_kmann.liquor_license'].aggregate(pipeline1))

# Construct the valid data from property assessments
for x in prop:
	# print(x['zipcode'])
	# Just in case fields are missing
	if not 'gross_tax' in x:
		continue
	if not 'zipcode' in x:
		continue
	if not 'land_sf' in x:
		continue
	sf = int(x['land_sf'])
	zipcode = x['zipcode']
	tax = int(x['gross_tax'])

	# cant calculate price per sf if its 0...
	if sf == 0:
		continue

	tax_per_sf = tax / sf
	entry = {"zipcode" : zipcode, "gross_tax" : tax, "y" : 1, "tax_per_sf" : tax_per_sf}
	# insert to temporary collection
	repo['jtsliu_kmann.zipcode_tax_count'].insert_one(entry)

# Aggregation to get the average tax per square foot by zipcode
pipeline2 = [
	{ "$group" : {"_id": "$zipcode", "tax_per_sf_sum" : {"$sum": "$tax_per_sf"}, "number_properties": {"$sum": 1}}},
	{ "$project" : {"avg_tax_per_sf": {"$divide": ["$tax_per_sf_sum", "$number_properties"]}, "number_properties": 1} }
]

zipcode_avg_prop_tax = list(repo['jtsliu_kmann.zipcode_tax_count'].aggregate(pipeline2))

# Combining the two data sets - basically a join, but mongo doesnt want to help :(
for location in zipcode_avg_prop_tax:
	desired = None
	for other in zipcode_liquor_locations:
		if location["_id"] == other["_id"]:
			desired = other
			break
	if not desired is None:
		location["liquor_locations"] = desired["liquor_locations"]

# Record the data
repo['jtsliu_kmann.zipcode_liquor_property_info'].insert_many(zipcode_avg_prop_tax)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

# Sorry for the long name...
this_script = doc.agent('alg:create_zipcode_with_liquor_and_property_value', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource1 = doc.entity('dat:liquor_license',
	{prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'}
)

resource2 = doc.entity('dat:property_assessment',
	{prov.model.PROV_LABEL: 'Property Assessment', prov.model.PROV_TYPE:'ont:Dataset'}
)

this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Computation'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)

# This was derived from 2 different data sets!
zipcode_data = doc.entity('dat:zipcode_liquor_property_info', {prov.model.PROV_LABEL:'Zipcode Liquor and Propery Data', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zipcode_data, this_script)
doc.wasGeneratedBy(zipcode_data, this_run, endTime)
doc.wasDerivedFrom(zipcode_data, resource1, this_run, this_run, this_run)
doc.wasDerivedFrom(zipcode_data, resource2, this_run, this_run, this_run)

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

