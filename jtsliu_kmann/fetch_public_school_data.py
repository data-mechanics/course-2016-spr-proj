# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script retrieves the public school information
import datetime
import json
import prov.model
import dml
import urllib.request
import uuid

# Set up the db connection
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

# Record start time
startTime = datetime.datetime.now()

url = "https://data.cityofboston.gov/resource/e29s-ympv.json?$limit=50000"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

repo.dropPermanent("public_schools")
repo.createPermanent("public_schools")

# I need to pad them with a 0 since every other set does this
clean = []
for elem in r:
	if not 'zipcode' in elem:
		# Apparently some of the data is missing this field
		continue
	elem['zipcode'] = '0' + str(elem['zipcode'])
	clean.append(elem)	

repo['jtsliu_kmann.public_schools'].insert_many(clean)

endTime = datetime.datetime.now()

# Create the provenance info
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:fetch_public_school_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource = doc.entity('bdp:ufcx-3fdn',
	{'prov:label': 'Boston Public Schools for school year 2012-2013',
	prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'}
)

this_run = doc.activity(
	'log:a'+str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE: 'ont:Retrieval', 'ont:Query': '?$limit=50000'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

public_schools = doc.entity('dat:public_schools', {prov.model.PROV_LABEL:'Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(public_schools, this_script)
doc.wasGeneratedBy(public_schools, this_run, endTime)
doc.wasDerivedFrom(public_schools, resource, this_run, this_run, this_run)

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

