# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script retrieves crime data from the city of boston
import datetime
import json
import prov.model
import pymongo
import urllib.request
import uuid

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Record start time
startTime = datetime.datetime.now()

# Reset data sets
repo.dropPermanent("crime")
repo.createPermanent("crime")

# Use the Socrata API to get the data - limited to 50000 records, we handle this using an offset
count = 50000
iterations = 0
while count == 50000: # once we don't get the max number, we have gotten them all
	url = "https://data.cityofboston.gov/resource/ufcx-3fdn.json?$limit=50000&$offset=" + str(50000 * iterations) 
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	count = len(r)
	iterations += 1
	# Filter out the bad data
	clean = []
	for elem in r:
		if elem['location']['coordinates'] != [0, 0]:
			clean.append(elem)

	# insert the clean data
	repo['jtsliu_kmann.crime'].insert_many(clean)

# record the end time
endTime = datetime.datetime.now()

# Create the provenance info
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:fetch_crime_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource = doc.entity('bdp:ufcx-3fdn',
	{'prov:label': 'Crime Incident Reports',
	prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'}
)

this_run = doc.activity(
	'log:a'+str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '?$limit=50000'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

crime = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crime Incidents', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crime, this_script)
doc.wasGeneratedBy(crime, this_run, endTime)
doc.wasDerivedFrom(crime, resource, this_run, this_run, this_run)

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

