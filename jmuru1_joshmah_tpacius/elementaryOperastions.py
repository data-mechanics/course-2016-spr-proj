import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import re
import apitest as apitest

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_tpacius', 'jmuru1_tpacius')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

#The collections are being created and populated here

#========================elementary ops=====================
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

def reduceNoFunction(K,R):
    keys = {k for (k,v) in K}
    return [(k1, [v for (k2,v) in R if int(k1) == int(k2)]) for k1 in keys]

# ========================query database functions=================================
def getCollection(dbName):
	temp = []
	if type(dbName) != str:
		return "Error: please input a string"
	for elem in repo['jmuru1_tpacius.' + dbName].find({}):
		temp.append(elem)
	return temp

# regex that extracts street names from streetjams
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

# regex extracting zipcodes from the hospital database
# ========================query database functions end =================================

# ===========================Perform ops on collections==============================
propertyValues = getCollection("propertyvalue")


def getKeys(propList):
    kk = [key for key, value in propList if key != "_id"]
    vk = [value for key, value in propList if value != "_id"]
    return kk + vk

#extracts the zipcodes and property values.
def zipcodeAggregate(propList):
    props = propList #takes in second part of each tuple (list of property)
    keys = getKeys(propList)
    dbInsert = {}
    for i in keys:
        dbInsert[i] = 0
    total = 0
    for k in keys:
        for i in range(len(props)):
            (key, value) = props[i]
            if k == key or k == value:
                if(not not props[i][k]):
                    for elem in props[i][k]:
                        dbInsert[k] += int(elem['av_total'])
    return dbInsert

    #extracts the zipcodes and property values.
def zipcodeLengthAggregate(propList):
    props = propList #takes in second part of each tuple (list of property)
    keys = getKeys(propList)
    dbInsert = {}
    for i in keys:
        dbInsert[i] = 0
    total = 0
    for k in keys:
        for i in range(len(props)):
            (key, value) = props[i]
            if k == key or k == value:
                if(not not props[i][k]):
                    for elem in props[i][k]:
                        dbInsert[k] += 1
    return dbInsert

def propertyAvg(proplist1, proplist2):
    props1 = proplist1[0]
    props2 = proplist2[0]
    dbInsert = {}
    for key1, value1 in props1.items():
        for key2, value2 in props2.items():
            if key1 == key2 and isinstance(value1, int) and isinstance(value2,int) and value1 != 0 :
                dbInsert[key1] = value1/value2
    return dbInsert



# ===========================Perform ops on collections end==============================
endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_joshmah_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_joshmah_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:ElementaryOperations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('dat:membersreduction', {'prov:label':'Members and Property Values Reduced By Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource2 = doc.entity('dat:reservationsreduction', {'prov:label':'Reservations and Property Values Reduced By Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(this_run, this_script)

doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
# print(doc.get_provn())
repo.logout()
