'''
file: getPoliceSalaryPlusCrimeIncidents.py
authors: Nicholas Louie (nlouie@bu.edu), Thomas Thsu (thsu8@bu.edu)
date 2/24/16

'''


import requests # import sodapy
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


def jsonGetAll(addr, limit = 50000, offset = 0):
	r = requests.get(addr + "&$limit=" + str(limit) + "&$offset=" + str(offset))
	if len(r.json()) < 1000:
		return r.json()
	else:
		j = r.json()
		offset += limit
		while len(r.json()) == limit:
			r = requests.get(addr + "&$limit=" + str(limit) + '&$offset=' + str(offset))
			j = j + r.json()
			offset += limit
			print(len(j))
		return j

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('nlouie_thsu8', 'nlouie_thsu8')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()


# Retrieve json files.
# Employee earnings 2012
j = jsonGetAll('https://data.cityofboston.gov/resource/effb-uspk.json')   # add auth token
repo.dropPermanent("earnings2012")
# insert into database
repo.createPermanent("earnings2012")
repo['nlouie_thsu8.earnings'].insert_many(r)


m = map(lambda k, v: [(2012, float(v['total_earnings']))] if v['department'] == 'Boston Police Department' else [], [("key", v) for v in j])
f = reduce(lambda k,vs: (k, sum(vs)/len(vs)), m)

# Retrieve json files.
j = jsonGetAll("https://data.cityofboston.gov/resource/7cdf-6fgx.json?")
m = map(lambda k, v: [(k, 1)], [(v['year'], v) for v in j])
f2 = reduce(lambda k, vs: (k, sum(vs)), m)


# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nlouie_thsu8/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/nlouie_thsu8/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:getPoliceSalaryPlusCrimeIncidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:effb-uspk', {'prov:label':'Employee Earnings Report 2012', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?department=Boston%20Police%20Department'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

lost = doc.entity('dat:lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(lost, this_script)
doc.wasGeneratedBy(lost, this_run, endTime)
doc.wasDerivedFrom(lost, resource, this_run, this_run, this_run)

found = doc.entity('dat:found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(found, this_script)
doc.wasGeneratedBy(found, this_run, endTime)
doc.wasDerivedFrom(found, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof











