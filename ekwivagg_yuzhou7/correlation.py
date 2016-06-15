import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
from geopy.distance import great_circle

# Set up the database connection.
auth = open('auth.json', 'r')
cred = json.load(auth)
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

larcenies = repo['ekwivagg_yuzhou7.crime_freq'].find()
liquors = repo['ekwivagg_yuzhou7.liquor_freq'].find()

lar_liq = {}
for larceny in larcenies:
	for key in larceny.keys():
		if key not in lar_liq.keys() and key != '_id':
			lar_liq[key] = [0, 0]
for liquor in liquors:
	for key in liquor.keys():
		if key != '_id':
			lar_liq[key] = [0, 0]

larcenies = repo['ekwivagg_yuzhou7.crime_freq'].find()
liquors = repo['ekwivagg_yuzhou7.liquor_freq'].find()

for larceny in larcenies:
	for key in larceny.keys():
		if key != '_id':
			if key in lar_liq.keys():
				lar_liq[key][0] = larceny[key]
for liquor in liquors:
	for key in liquor.keys():
		if key != '_id':
			if key in lar_liq.keys():
				lar_liq[key][1] = liquor[key]


with open('correlation.csv', 'w') as out:
    fieldnames = ['Liquor', 'Larceny', 'T_Stop']
    dw = csv.DictWriter(out, fieldnames=fieldnames)
    dw.writeheader()
    for key in lar_liq.keys():
        dw.writerow({'Liquor': lar_liq[key][1], 'Larceny': lar_liq[key][0], 'T_Stop':key})

endTime = datetime.datetime.now()

# Provenance information for plan.jason
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ekwivagg_yuzhou7/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ekwivagg_yuzhou7/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crime_dat = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crime', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
crime_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(crime_retrieval, this_script)
doc.used(crime_retrieval, crime_dat, startTime)

liquor_dat = doc.entity('dat:liquor', {prov.model.PROV_LABEL:'Liquor', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
liquor_retrieval = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
doc.wasAssociatedWith(liquor_retrieval, this_script)
doc.used(liquor_retrieval, liquor_dat, startTime)

repo.record(doc.serialize())
content = json.dumps(json.loads(doc.serialize()), indent=4)
f = open('plan.json', 'a')
f.write(",\n")
f.write(content)
repo.logout()
