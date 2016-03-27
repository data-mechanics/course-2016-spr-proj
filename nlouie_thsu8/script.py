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

Description: The script opens the nlouie_thsu8 Mongo Database, takes the data from our datamechanics repo, and adds
the original and processed data to the database
'''

import requests # import sodapy
import json
import pymongo
import prov.model
import datetime
import uuid
import urllib

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

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

# take collection name (same as dataset name without .json at end)
def insert_to_db(repo, s):
    url = 'https://data-mechanics.s3.amazonaws.com/nlouie_thsu8/data/' + s + '.json'
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    
    # The line below was causing an error.
    #r = json.dumps(r, sort_keys=True, indent=2)

    repo.dropPermanent(s)
    repo.createPermanent(s)

    # Convert to valid JSON dictionary if necessary.
    for i in range(len(r)):
        if type(r[i]) == list:
            d = {}
            d[r[i][0]] = r[i][1]
            r[i] = d

    repo['nlouie_thsu8.' + s].insert_many(r)

# add to mongo database and generate prov
def main():

    # Until a library is created, we just use the script directly.
    exec(open('../pymongo_dm.py').read())

    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('nlouie_thsu8', 'nlouie_thsu8')

    # Retrieve some data sets (not using the API here for the sake of simplicity).
    startTime = datetime.datetime.now()

    # insert entries into mongo database

    insert_to_db(repo, 'BPDEarnings2012')
    insert_to_db(repo, 'BPDEarnings2013')
    insert_to_db(repo, 'BPDEarnings2014')
    insert_to_db(repo, 'BPDEarnings2012')
    insert_to_db(repo, 'avgEarnings')
    insert_to_db(repo, 'incidentCounts')
    insert_to_db(repo, 'avgEarningsIncidents')

    endTime = datetime.datetime.now()

    # generate provenance data

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nlouie_thsu8/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nlouie_thsu8/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bdp', 'https://data-mechanics.s3.amazonaws.com/nlouie_thsu8/data')

    this_script = doc.agent('alg:script', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # resources

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

    # generated

    avgEarnings = doc.entity('dat:avgEarnings', {prov.model.PROV_LABEL:'Police Salary and Crime Incidence by Year', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(avgEarnings, this_script)
    doc.wasGeneratedBy(avgEarnings, this_run0, endTime)
    doc.wasGeneratedBy(avgEarnings, this_run1, endTime)
    doc.wasGeneratedBy(avgEarnings, this_run2, endTime)
    doc.wasDerivedFrom(avgEarnings, resource0, this_run0)
    doc.wasDerivedFrom(avgEarnings, resource1, this_run1)
    doc.wasDerivedFrom(avgEarnings, resource2, this_run2)

    incidentCounts = doc.entity('dat:incidentCounts', {prov.model.PROV_LABEL:'Police Salary and Crime Incidence by Year', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(incidentCounts, this_script)
    doc.wasGeneratedBy(incidentCounts, this_run3, endTime)
    doc.wasDerivedFrom(incidentCounts, resource3, this_run3)

    avgEarningsIncidents = doc.entity('dat:avgEarningsIncidents', {prov.model.PROV_LABEL:'Police Salary and Crime Incidence by Year', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(avgEarningsIncidents, this_script)
    doc.wasGeneratedBy(avgEarningsIncidents, this_run3, endTime)
    doc.wasDerivedFrom(avgEarningsIncidents, resource3, this_run0, this_run1, this_run2)

    repo.record(doc.serialize()) # Record the provenance document.
    #print(json.dumps(json.loads(doc.serialize()), indent=4))
    open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
    print(doc.get_provn())
    repo.logout()

if __name__ == '__main__':
    main()
