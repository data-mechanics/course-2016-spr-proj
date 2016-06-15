# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script retrieves the hospital information
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

url = "https://data.cityofboston.gov/resource/46f7-2snz.json"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

repo.dropPermanent("hospitals")
repo.createPermanent("hospitals")

# I need to pad them with a 0 since every other set does this
clean = []
for elem in r:
	if not 'zipcode' in elem:
		# Apparently some of the data is missing this field
		continue
	elem['zipcode'] = '0' + str(elem['zipcode'])
	clean.append(elem)	

repo['jtsliu_kmann.hospitals'].insert_many(clean)

endTime = datetime.datetime.now()

# Create the provenance info
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in / format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in / format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:fetch_hospital_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
resource = doc.entity('bdp:ufcx-3fdn',
	{'prov:label': 'Hospital Locations',
	prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'}
)

this_run = doc.activity(
	'log:a'+str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE: 'ont:Retrieval'}
)

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

hospitals = doc.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(hospitals, this_script)
doc.wasGeneratedBy(hospitals, this_run, endTime)
doc.wasDerivedFrom(hospitals, resource, this_run, this_run, this_run)

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

