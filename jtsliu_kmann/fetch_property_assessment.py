# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script retrieves property assessment data from the city of boston
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

# record start time
startTime = datetime.datetime.now()

repo.dropPermanent("property_assessment")
repo.createPermanent("property_assessment")

# Socrata API limits to 50000, use offset to get around this and get all the records
count = 50000
iteration = 0
while count == 50000:
	url = "https://data.cityofboston.gov/resource/qz7u-kb7x.json?$limit=50000&$offset=" + str(50000 * iteration)
	response = urllib.request.urlopen(url).read().decode("utf-8")
	r = json.loads(response)
	s = json.dumps(r, sort_keys=True, indent=2)
	iteration += 1
	count = len(r)
	print("added", len(r), "records")
	repo['jtsliu_kmann.property_assessment'].insert_many(r)

endTime = datetime.datetime.now()

# create provenance
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:fetch_property_assessment', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource = doc.entity('bdp:qz7u-kb7x',
	{'prov:label': 'Property Assessment 2014',
	prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'}
)
this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query': '?$limit=50000'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

property_assessment = doc.entity('dat:property_assessment', {prov.model.PROV_LABEL: 'Property Assessment', prov.model.PROV_TYPE:'ont:Dataset'})
doc.wasAttributedTo(property_assessment, this_script)
doc.wasGeneratedBy(property_assessment, this_run, endTime)
doc.wasDerivedFrom(property_assessment, resource, this_run, this_run, this_run)

repo.record(doc.serialize())

# update plan.json
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




