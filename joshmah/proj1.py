import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

#Prof. Lapet's code used for intersections
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
x = repo['joshmah.streetjams'].find({});

# Location of Hospitals
#url = https://data.cityofboston.gov/api/views/46f7-2snz/rows.json

url = "https://data.cityofboston.gov/resource/46f7-2snz.json?"
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)


repo.dropPermanent("hospitals")
repo.createPermanent("hospitals")
repo['joshmah.hospitals'].insert_many(r)


#Parse through dataases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['joshmah.' + dbName].find({}):
		temp.append(elem)
	return temp


#Pulling from the database for hospitals
x = getCollection('hospitals')
X = []
Xnames = []


for i in range(len(x)):
    X.append(x[i]['ad'])
    
    
y = getCollection('streetjams')
Y = []

#Pulling from the database for streetjams...could also add endnodes in the future
for i in range(len(y)):
    if('street' in y[i]):
        Y.append(y[i]['street'])
        

    if('endnode' in y[i]):
        Y.append(y[i]['endnode'])
        
        
#Regular expression removal for hospital addressess to match streets.
#Removes all the numbers from the streets, and spaces from the beginning.
import re
Xnew = []
for i in range(len(X)):
    temp = (re.sub(r'[0-9]', '', X[i]))
    temp = (re.sub(r"^\s+", "", temp))
    
    #For some reason...hospitals view their Avenues as "AV"
    #Regular expression to adjust for it.    
    temp = re.sub(r'AV','AVE',temp)
    #Some are capitalized, some aren't...standardization.
    Xnew.append(temp.upper())




for i in range(len(x)):
    temp = (re.sub(r'\.','',x[i]['name']))
    Xnames.append((Xnew[i],temp))
Xnames2 = dict((x,y) for y, x in Xnames)
print(Xnames2)

repo.dropPermanent("intersectionsHospitalsStreets")
repo.createPermanent("intersectionsHospitalsStreets")
repo['joshmah.intersectionsHospitalsStreets'].insert_one(Xnames2)

#Useful if you want to see which hospitals are on which streets.
#print(Xnames2)

#print(Xnames)

#Capitalizing everything for streetjams address
Ynew = []
for i in range(len(Y)):
    Ynew.append(Y[i].upper())
Y = Ynew
    
#Find the intersections between streetjames and hospitals...for number of hospitals.
X = intersect(Xnew,Ynew)
Y = intersect(Ynew,Xnew)



#Number of hospitals on streets that have intersections
X = dict((x,X.count(x)) for x in set(X))
print(X)

#Put it into the dict with a count of how many intersections occur.
Y = dict((x,Y.count(x)) for x in Y)
print(Y)




#Insert into a new database.

repo.dropPermanent("intersectionsJamsHospitals")
repo.createPermanent("intersectionsJamsHospitals")
repo['joshmah.intersectionsJamsHospitals'].insert_one(Y)


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



this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceHospitals = doc.entity('bdp:46f7-2snz', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resourceStreetjams = doc.entity('bdp:yqgx-2ktq', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

get_hospitals = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=ad&?$select=ad,name'})
get_streetjams = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?$select=endnode'})
doc.wasAssociatedWith(get_hospitals, this_script)
doc.wasAssociatedWith(get_streetjams, this_script)
doc.used(get_streetjams, resourceStreetjams, startTime)
doc.used(get_hospitals, resourceHospitals, startTime)

hospitals = doc.entity('dat:hospitals', {prov.model.PROV_LABEL:'ad', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(hospitals, this_script)
doc.wasGeneratedBy(hospitals, get_hospitals, endTime)
doc.wasDerivedFrom(hospitals, resourceHospitals, get_hospitals, get_hospitals, get_hospitals)

streetjams = doc.entity('dat:streetjams', {prov.model.PROV_LABEL:'street', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(streetjams, this_script)
doc.wasGeneratedBy(streetjams, get_streetjams, endTime)
doc.wasDerivedFrom(streetjams, resourceStreetjams, get_streetjams, get_streetjams, get_streetjams)


repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


#Code from Lecture Notes

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
## eof