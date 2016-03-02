# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# This script retrieves liquor license data from the city of boston
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

url = "https://data.cityofboston.gov/resource/hda6-fnsh.json?$limit=50000"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("liquor_license")
repo.createPermanent("liquor_license")
# clean the data
clean = []
for elem in r:
	if not 'location' in elem:
		# Apparently some of the data is missing this field
		continue
	# They are stored as strings which is super cool
	if float(elem['location']['latitude']) != 0 or float(elem['location']['longitude']) != 0:
		clean.append(elem)
# insert the clean data
repo['jtsliu_kmann.liquor_license'].insert_many(clean)

# record end time
endTime = datetime.datetime.now()

# Creating the provenance
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jtsliu_kmann/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jtsliu_kmann/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:fetch_liquor_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:hda6-fnsh',
	{'prov:label':'Liquor Licenses',
	prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'} )

this_run = doc.activity(
	'log:a' + str(uuid.uuid4()), startTime, endTime,
	{prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query': '?$limit=50000'})

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

found = doc.entity('dat:liquor_license', {prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(found, this_script)
doc.wasGeneratedBy(found, this_run, endTime)
doc.wasDerivedFrom(found, resource, this_run, this_run, this_run)

# write to the plan.json
repo.record(doc.serialize())
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()




