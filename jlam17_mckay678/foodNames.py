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
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
#repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

filen = 'http://upload.datamechanics.org/?prefix=jlam17_mckay678/data/activeFood.json'
res = open(filen, 'r')
r = json.load(res)
repo.dropPermanent("foodEst")
repo.createPermanent("foodEst")
repo['jlam17_mckay678.foodEst'].insert_many(r)

filen = 'http://upload.datamechanics.org/?prefix=jlam17_mckay678/data/cornerStores.json'
res = open(filen, 'r')
r2 = json.load(res)
repo.dropPermanent("cornerStore")
repo.createPermanent("cornerStore")
repo['jlam17_mckay678.cornerStore'].insert_many(r2)

filen = 'http://upload.datamechanics.org/?prefix=jlam17_mckay678/data/foodPantry.json'
res = open(filen, 'r')
r3 = json.load(res)
repo.dropPermanent("foodPantry")
repo.createPermanent("foodPantry")
repo['jlam17_mckay678.foodPantry'].insert_many(r3)

filen = 'http://upload.datamechanics.org/?prefix=jlam17_mckay678/data/summerFM.json'
res = open(filen, encoding="utf8")
r4 = json.load(res)
repo.dropPermanent("summerFM")
repo.createPermanent("summerFM")
repo['jlam17_mckay678.summerFM'].insert_many(r4)

filen = 'http://upload.datamechanics.org/?prefix=jlam17_mckay678/data/retailBakery.json'
res = open(filen, encoding="utf8")
r5 = json.load(res)
repo.dropPermanent("retailBakery")
repo.createPermanent("retailBakery")
repo['jlam17_mckay678.retailBakery'].insert_many(r5)

# repo.dropPermanent("food")
# repo.createPermanent("food")
# repo['jlam17_mckay678.food'].insert_many(r)
# repo['jlam17_mckay678.food'].insert_many(r2)

#Create a database for all the food places together.
repo.dropTemp("foodNames")
repo.createTemp("foodNames")
title = ''

#Pick out the names of the places and puts them in foodNames
for idx, u in enumerate(r):
	if idx == 0:
		title = r[idx]['FIELD1']
	elif r[idx]['FIELD1']:
		repo['jlam17_mckay678.foodNames'].insert({'Name': r[idx]['FIELD1']})

for idx, u in enumerate(r2):
	if idx == 0:
		title = r2[idx]['FIELD1']
	elif r2[idx]['FIELD1']:
		repo['jlam17_mckay678.foodNames'].insert({'Name': r2[idx]['FIELD1']})

for idx, u in enumerate(r3):
	if idx == 0:
		title = r3[idx]['FIELD2']
	elif r[idx]['FIELD2']:
		repo['jlam17_mckay678.foodNames'].insert({'Name': r3[idx]['FIELD2']})

for idx, u in enumerate(r4):
	if idx == 0:
		title = r4[idx]['FIELD1']
	elif r[idx]['FIELD1']:
		repo['jlam17_mckay678.foodNames'].insert({'Name': r4[idx]['FIELD1']})

for idx, u in enumerate(r5):
	if idx == 0:
		title = r5[idx]['FIELD1']
	elif r[idx]['FIELD1']:
		repo['jlam17_mckay678.foodNames'].insert({'Name': r5[idx]['FIELD1']})


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

this_script = doc.agent('alg:example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Query':'?accessType=DOWNLOAD'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

foodEst = doc.entity('dat:foodEst', {prov.model.PROV_LABEL:'foodEst', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(foodEst, this_script)
doc.wasGeneratedBy(foodEst, this_run, endTime)
doc.wasDerivedFrom(foodEst, resource, this_run, this_run, this_run)

cornerStore = doc.entity('dat:cornerStore', {prov.model.PROV_LABEL:'cornerStore', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(cornerStore, this_script)
doc.wasGeneratedBy(cornerStore, this_run, endTime)
doc.wasDerivedFrom(cornerStore, resource, this_run, this_run, this_run)

foodPantry = doc.entity('dat:foodPantry', {prov.model.PROV_LABEL:'foodPantry', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(foodPantry, this_script)
doc.wasGeneratedBy(foodPantry, this_run, endTime)
doc.wasDerivedFrom(foodPantry, resource, this_run, this_run, this_run)

summerFM = doc.entity('dat:summerFM', {prov.model.PROV_LABEL:'summerFM', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(summerFM, this_script)
doc.wasGeneratedBy(summerFM, this_run, endTime)
doc.wasDerivedFrom(summerFM, resource, this_run, this_run, this_run)

retailBakery = doc.entity('dat:retailBakery', {prov.model.PROV_LABEL:'retailBakery', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(retailBakery, this_script)
doc.wasGeneratedBy(retailBakery, this_run, endTime)
doc.wasDerivedFrom(retailBakery, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
provEx = open('http://upload.datamechanics.org/?prefix=jlam17_mckay678/provFoodNames.json', 'w')
provEx.write(json.dumps(json.loads(doc.serialize()), indent=4))
prov2 = open('http://upload.datamechanics.org/?prefix=jlam17_mckay678/planFoodNames.json', 'w')
provEx.write(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
# print(doc.get_provn())
repo.logout()

## eof

