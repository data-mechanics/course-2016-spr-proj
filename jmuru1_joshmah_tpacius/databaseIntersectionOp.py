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
repo.authenticate('jmuru1_joshmah_tpacius', 'jmuru1_joshmah_tpacius')
startTime = datetime.datetime.now()

#Parse through databases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['jmuru1_joshmah_tpacius.' + dbName].find({}):
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
# print(Xnames2)

repo.dropPermanent("intersectionsHospitalsStreets")
repo.createPermanent("intersectionsHospitalsStreets")
repo['jmuru1_joshmah_tpacius.intersectionsHospitalsStreets'].insert_one(Xnames2)

#Useful if you want to see which hospitals are on which streets.
#print(Xnames2)

# print(Xnames)

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
# print(X)

#Put it into the dict with a count of how many intersections occur.
Y = dict((x,Y.count(x)) for x in Y)
# print(Y)

#Intersection over hospital street names and counts

#Match traffic jam count to hospital names
def patternMatchRoads(R, S):
    result = []
    for elem in R:
        (k1,v1) = elem
        for k2, v2 in S.items():
            if k1 == k2:
                result.append((v1,v2))
    return result


Z = dict(patternMatchRoads(Xnames, Y))
# print(Z)

#Insert into a new database.

repo.dropPermanent("intersectionsJamsHospitals")
repo.createPermanent("intersectionsJamsHospitals")
repo['jmuru1_joshmah_tpacius.intersectionsJamsHospitals'].insert_one(Y)

#ADD PROVENANCE!?
repo.dropPermanent("hospital_jams_count")
repo.createPermanent("hospital_jams_count")
repo['jmuru1_joshmah_tpacius.hospital_jams_count'].insert_one(Z)


endTime = datetime.datetime.now()


# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.

# Taken from example.py, chaning alice_bob to joshmah'jmuru1_joshmah_tpacius'
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_joshmah_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_joshmah_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')



this_script = doc.agent('alg:databaseIntersectionOp', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('dat:intersectionsHospitalsStreets', {'prov:label':'Hospitals Addresses and Traffic Intersections Reduced by Street', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource2 = doc.entity('dat:intersectionsJamsHospitals', {'prov:label':'Traffic Jams and Hospitals Addresses Reduced by Street', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource3 = doc.entity('dat:hospital_jams_count', {'prov:label':'Hospitals and Nearby Traffic Jam Counts', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(this_run, this_script)

doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)
doc.used(this_run, resource3, startTime)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof