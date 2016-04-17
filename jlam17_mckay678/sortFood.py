import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()


# repo.dropPermanent("food")
# repo.createPermanent("food")
# repo['jlam17_mckay678.food'].insert_many(r)
# repo['jlam17_mckay678.food'].insert_many(r2)

repo.dropPermanent("sortedFood")
repo.createPermanent("sortedFood")
title = ''
for i in repo['jlam17_mckay678.fixedFood']:
	if i['Neighborhood'] == "Allston/Brighton":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Back Bay":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Central":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Charlestown":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "East Boston":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Fenway/Kenmore":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Harbor Islands":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Hyde Park":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Jamaica Plain":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Mattapan":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "North Dorchester":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Roslindale":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "Roxbury":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "South Boston":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "South Dorchester":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "South End":
		repo['jlam17_mckay678.sortedFood'].insert(i)
	elif i['Neighborhood'] == "West Roxbury":
		repo['jlam17_mckay678.sortedFood'].insert(i)


endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jlam17_mckay678/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jlam17_mckay678/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:sortFood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Sort Fixed Foods', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Query':'?accessType=DOWNLOAD'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

fixedFood = doc.entity('dat:fixedFood', {prov.model.PROV_LABEL:'fixedFood', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(fixedFood, this_script)
doc.wasGeneratedBy(fixedFood, this_run, endTime)
doc.wasDerivedFrom(fixedFood, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
provEx = open('provSortFood.json', 'w')
provEx.write(json.dumps(json.loads(doc.serialize()), indent=4))
prov2 = open('plan.json', 'a')
prov2.write(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
# print(doc.get_provn())
repo.logout()

## eof
