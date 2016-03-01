#Jam Data from Waze
#Hospital data

import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

#Prof. Lapet's code
def intersect(R, S):
    return [t for t in R if t in S]
    
# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('joshmah', 'joshmah')


startTime = datetime.datetime.now()

#Json file for Waze Jam Data
#url = 'https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json'
url = 'https://data.cityofboston.gov/resource/yqgx-2ktq.json?'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("streetjams")
repo.createPermanent("streetjams")
repo['joshmah.streetjams'].insert_many(r)

#EMS 911 Data
#url = 'https://data.cityofboston.gov/api/views/ak6k-a5up/rows.json$limit=5'

url = "https://data.cityofboston.gov/resource/ak6k-a5up.json?"

response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

repo.dropPermanent("emsdeparture")
repo.createPermanent("emsdeparture")
repo['joshmah.emsdeparture'].insert_many(r)

def getCollection(dbName):
	temp = []
	for elem in repo['joshmah.' + dbName].find({}):
		temp.append(elem)
	return temp

#Has to be sets because there are a lot of duplicates

x = getCollection('emsdeparture')
X = []
for i in range(len(x)):
    X.append(x[i]['start_standard_time'])
    
y = getCollection('streetjams')
Y = []
for i in range(len(y)):
    Y.append(y[i]['starttime'])
    
import re

    
Xnew = set()
Xnew2 = []
for i in range(len(X)):
    temp = (re.sub(r'[A-Z]', '', X[i]))
    temp = (re.sub(r"\s+", "", temp))
    Xnew.add(temp)
for i in range(len(X)):
    temp = (re.sub(r'[A-Z]', '', X[i]))
    temp = (re.sub(r"\s+", "", temp))
    temp = temp[0:5]
    Xnew2.append(temp)
Xnew2 = sorted(Xnew2)
#Xnew = sorted(Xnew2)


Ylist = []

for i in range(len(Y)):
    temp = re.sub(r'2015-02-23T','',Y[i])
    temp = temp[0:5]
    Ylist.append(temp)

#Sorted them for pretty.
Ylist = sorted(Ylist)

Y = intersect(Ylist, Xnew2)

#Wrong comparison...from # of traffic jams (much more data)
#into number of ems departures.
X = dict((x,X.count(x)) for x in Xnew2)
#print(X)

Ywithintersect = dict((x,Y.count(x)) for x in Y)
print(Ywithintersect)
#Number of ambulance times and number of traffic jams at that time!
Y = dict((x,Y.count(x)) for x in Ylist)
print(Y)
#Insert into a new database.
repo.dropPermanent("intersectionsJamsAmbulances")
repo.createPermanent("intersectionsJamsAmbulances")
repo['joshmah.intersectionsJamsAmbulances'].insert_one(Y)

#
#Ycounter = []*len(Ylist)
#for x in range (len(Ylist)):
#    for y in range (len(Xnew)):
#        if(Ylist - Xnew):
#            Ycounter[y] = Ycounter[y] + 1
            
#print(Xnew)
#print("\n\n")
#print(Ylist)

endTime = datetime.datetime.now()
# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.

# Taken from example.py, chaning alice_bob to joshmah
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/joshmah/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/joshmah/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')



this_script = doc.agent('alg:proj1partb', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceEMSdeparture = doc.entity('bdp:ak6k-a5up', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

this_script = doc.agent('alg:proj1partb', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceStreetjams = doc.entity('bdp:yqgx-2ktq', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_emsdepartures = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=ad&?$select=ad,name'})
get_streetjams = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=endnode'})
doc.wasAssociatedWith(get_emsdepartures, this_script)
doc.wasAssociatedWith(get_streetjams, this_script)
doc.used(get_streetjams, resourceStreetjams, startTime)
doc.used(get_emsdepartures, resourceEMSdeparture, startTime)

EMSdeparture = doc.entity('dat:emsdepartures', {prov.model.PROV_LABEL:'start_standard_time',prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(EMSdeparture, this_script)
doc.wasGeneratedBy(EMSdeparture, get_emsdepartures, endTime)
doc.wasDerivedFrom(EMSdeparture, resourceEMSdeparture, get_emsdepartures, get_emsdepartures, get_emsdepartures)

streetjams = doc.entity('dat:streetjams', {prov.model.PROV_LABEL:'starttime', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(streetjams, this_script)
doc.wasGeneratedBy(streetjams, get_streetjams, endTime)
doc.wasDerivedFrom(streetjams, resourceStreetjams, get_streetjams, get_streetjams, get_streetjams)


repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan2.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

repo.logout()


def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]
    
def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]