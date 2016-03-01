'''
Nicholas Louie, Thomas Hsu
nlouie@bu.edu, thsu@bu.edu
nlouie_thsu8
2/27/16
Boston University Department of Computer Science
CS 591 L1 - Data Mechanics Project 1
Andrei Lapets (lapets@bu.edu)
Datamechanics.org
Datamechanics.io

Description: The script opens the nlouie_thsu8 Mongo Database, takes the data off Boston's dataset without needing to the use the API key.
We take the dataset from Boston's Salaries (2012,2013,2014) , reduce for the Boston Police Department and map each with the amount of crime incidents
for that year.
'''

import requests # import sodapy
import json
import pymongo
import prov.model
import datetime
import uuid
import functools


def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]


def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]


# gets the entire dataset.


def json_get_all(addr, limit=50000, offset=0):
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


def dict_merge(x, y):
    z = x.copy()
    z.update(y)
    return z

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('nlouie_thsu8', 'nlouie_thsu8')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

'''
# Retrieve json files. 
j = json_get_all('https://data.cityofboston.gov/resource/effb-uspk.json?department=Boston%20Police%20Department')
# Map the salary of a police man to the year. 
m = map(lambda k, v: [('2012', float(v['total_earnings']))] if v['department'] == 'Boston Police Department' else [], [("key", v) for v in j])


# Retrieve json files.
j = json_get_all('https://data.cityofboston.gov/resource/54s2-yxpg.json?department=Boston%20Police%20Department')
# Map the salary of a police man to the year. 
m = m + map(lambda k, v: [('2013', float(v['total_earnings']))] if v['department'] == 'Boston Police Department' else [], [("key", v) for v in j])


# Retrieve json files.
j = json_get_all('https://data.cityofboston.gov/resource/4swk-wcg8.json?department_name=Boston%20Police%20Department')
# Map the salary of a police man to the year. Note: accounts for inconsident field labling.
m = m + map(lambda k, v: [('2014', float(v['total_earnings']))] if v['department_name'] == 'Boston Police Department' else [], [("key", v) for v in j])


# Average the salary for each year. 
f = reduce(lambda k,vs: (k, {'avg_salary': sum(vs)/len(vs)}), m)

print(f)

# Retrieve json files. This takes a longgggg time. Just take my word for it. 
#j = jsonGetAll("https://data.cityofboston.gov/resource/7cdf-6fgx.json?")
# Map an incident to the year with a value of 1. 
# m = map(lambda k, v: [(k, 1)], [(v['year'], v) for v in j])
# Reduce by year. 
# f3 = reduce(lambda k, vs: (k, sum(vs)), m)

f3 = [('2014', 88058), ('2015', 49760), ('2013', 87052), ('2012', 43186)]
f3 = map(lambda k, v: [(k, {'incidence_count': v})], f3)

print(f3)

ff = reduce(lambda k, vs: dict_merge({'year': k}, functools.reduce(dict_merge, vs)), f + f3)
print(ff)
'''

# insert entries into mongo database

repo.dropPermanent('policeSalaryToCrimeIncidence')
repo.createPermanent('policeSalaryToCrimeIncidence')
repo['nlouie_thsu8.policeSalaryToCrimeIncidence'].insert_many(ff)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nlouie_thsu8/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/nlouie_thsu8/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data-mechanics.s3.amazonaws.com')

this_script = doc.agent('alg:script', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

resource0 = doc.entity('bdp:effb-uspk', {'prov:label':'Employee Earnings Report 2012', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run0 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?department=Boston%20Police%20Department'})
doc.wasAssociatedWith(this_run0, this_script)
doc.used(this_run0, resource0, startTime)

resource1 = doc.entity('bdp:54s2-yxpg', {'prov:label':'Employee Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run1 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?department=Boston%20Police%20Department'})
doc.wasAssociatedWith(this_run1, this_script)
doc.used(this_run1, resource1, startTime)

resource2 = doc.entity('bdp:4swk-wcg8', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run2 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?department_name=Boston%20Police%20Department'})
doc.wasAssociatedWith(this_run2, this_script)
doc.used(this_run2, resource2, startTime)

resource3 = doc.entity('bdp:7cdf-6fgx', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run3 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})
doc.wasAssociatedWith(this_run3, this_script)
doc.used(this_run3, resource3, startTime)

lost = doc.entity('dat:policeSalaryToCrimeIncidence', {prov.model.PROV_LABEL:'Police Salary and Crime Incidence by Year', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(lost, this_script)
doc.wasGeneratedBy(lost, this_run3, endTime)
doc.wasDerivedFrom(lost, resource3, this_run0, this_run1, this_run2)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()





