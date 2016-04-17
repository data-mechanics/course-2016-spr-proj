# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script generates crimes that are potentially related to a liquor license 
# Note: we are attributing a crime to the nearest license's zipcode; however,
# this does not mean that the crime is necessarily occured in that zipcode, but it is
# 'attributed' to the zipcode in this way
import datetime
import json
import prov.model
import pymongo
import re
import urllib.request
import uuid
from bson.son import SON

# This is a semi-arbitrary parameter for determing if something was alcohol related
# Since the area of Boston is relatively small (in terms of the whole earth)
# we assume that euclidean distance is pretty accurate. Furthermore, this is around 2.5 - 3 miles
PSEUDO_WALKING_DISTANCE = .05

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

def calculate_distance(point1, point2):
	return ( (point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2 ) ** (.5)

def has_alcohol_vendor(point1, streetName):
	reg_name = streetName
	elems = repo['jtsliu_kmann.liquor_capitalized'].find({"address" : {'$regex' : reg_name}})
	min_distance = 99999999
	best_location = None
	for elem in elems:
		# print("YAY", elem)
		# print()
		point2 = (float(elem["location"]["longitude"]), float(elem["location"]["latitude"]))
		distance = calculate_distance(point1, point2)
		if distance < min_distance and distance < PSEUDO_WALKING_DISTANCE:
			min_distance = distance
			best_location = elem

	return best_location


# Start the process
startTime = datetime.datetime.now()

# Temporary collections for computations
repo.dropTemporary("liquor_capitalized")
repo.createTemporary("liquor_capitalized")

repo.dropPermanent("liquor_influenced_crime")
repo.createPermanent("liquor_influenced_crime")


crime = getCollection('crime')
liquor = getCollection('liquor_license')
liquor_capitalized = []

for l in liquor:
	l["address"] = l["address"].strip().upper()
	liquor_capitalized.append(l)
	repo['jtsliu_kmann.liquor_capitalized'].insert_one(l)

# put in the distance, zipcode, and potentially whole store
# TODO: potentially turn this into a map operation - but ehhhh
for c in crime:
	original_street = c["streetname"]
	result = has_alcohol_vendor(c["location"]["coordinates"], original_street.split()[0])
	if result is None:
		continue
	c["zip"] = result["zip"]
	c["liquor_license"] = result
	repo['jtsliu_kmann.liquor_influenced_crime'].insert_one(c)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:create_crime_near_alcohol', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

resource1 = doc.entity('dat:liquor_license',
	{prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'}
)

resource2 = doc.entity('dat:crime', 
	{prov.model.PROV_LABEL:'Crime Incidents', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Computation'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)

crime_alcohol = doc.entity('dat:liquor_influenced_crime', {prov.model.PROV_LABEL:'Crime near Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crime_alcohol, this_script)
doc.wasGeneratedBy(crime_alcohol, this_run, endTime)
doc.wasDerivedFrom(crime_alcohol, resource1, this_run, this_run, this_run)
doc.wasDerivedFrom(crime_alcohol, resource2, this_run, this_run, this_run)

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



# TODO: drop temorary collections





