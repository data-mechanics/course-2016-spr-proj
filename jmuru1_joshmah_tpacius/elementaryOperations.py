import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
import apiTest as apiTest
from math import ceil

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_joshmah_tpacius', 'jmuru1_joshmah_tpacius')

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
	for elem in repo['jmuru1_joshmah_tpacius.' + dbName].find({}):
		temp.append(elem)
	return temp
# ========================query database functions end =================================

# ===========================Perform ops on collections==============================
propertyValues = getCollection("propertyvalue")
hospitalCollection = getCollection("hospitals")
streetJamCollection = getCollection("streetjams")
intersectHJ = getCollection("hospital_jams_count")

#Gets Hospital name and long/lat
def extractHosptialLocations(collection):
    dbInsert = {}
    for elem in collection:
        name = elem['name']
        if name == "St. Elizabeth's Hospital":
            name = "St Elizabeth's Hospital"
        if name == "St. Margaret's Hospital For Women":
            name = "St Margaret's Hospital For Women"
        dbInsert[name] = (elem['location']['longitude'], elem['location']['latitude'])
    return dbInsert

# reduce the property value collection onto hospital zipcodes
def collectionsReducePropsByZip(a, compareCollection=propertyValues):
    h = [(addZero(hospiZip["zipcode"]), hospiZip) for hospiZip in a] # get hospital zips
    c = [(propertyPostal['zipcode'], propertyPostal) for propertyPostal in compareCollection] #property values
    reduction = reduceNoFunction(h,c)
     #return reduction between property values and hospitals
    return reduction

def addZero(hz):
    temp = '0' + hz
    return temp

# reduce the hospital location into property zip code
def collectionsReduceHopsByZip(a, compareCollection=propertyValues):
    h = [(addZero(hospiZip["zipcode"]), hospiZip) for hospiZip in a] # get hospital zips
    c = [(propertyPostal['zipcode'], propertyPostal) for propertyPostal in compareCollection] #property values
    reduction = reduceNoFunction(c,h)
     #return reduction between property values and hospitals
    return reduction

# helper to circumvent MongoDB not ordering response and retrieve keys
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

#calculate average property value
def propertyAvg(proplist1, proplist2):
    props1 = proplist1
    props2 = proplist2
    dbInsert = {}
    for key1, value1 in props1.items():
        for key2, value2 in props2.items():
            if key1 == key2 and isinstance(value1, int) and isinstance(value2,int) and value1 != 0 :
                dbInsert[key1] = ceil(value1/value2)
    return dbInsert

#Helper to extract keys
def getKeys2(propList):
    kk = [key for key, value in propList if key != "_id"]
    return kk
#
def zipcodeNameAggregate(propList):
    props = propList #takes in second part of each tuple (list of property)
    keys = getKeys2(propList)
    dbInsert = {}
    for i in keys:
        dbInsert[i] = []
    for elem in props:
        zips, lst = elem
        for hospital in lst:
            if hospital['name'] == "St. Elizabeth's Hospital":
                dbInsert[zips].append("St Elizabeth's Hospital")
            elif hospital['name'] == "St. Margaret's Hospital For Women":
                dbInsert[zips].append("St Margaret's Hospital For Women")
            else:
                dbInsert[zips].append(hospital['name'])
    return dbInsert

#======================================== save to database code =====================================
property_zips_reduction = collectionsReducePropsByZip(hospitalCollection)
repo.dropPermanent("property_zips_reduction")
repo.createPermanent("property_zips_reduction")
for elem in property_zips_reduction:
    d = {elem[0]: elem[1]}
    repo['jmuru1_joshmah_tpacius.property_zips_reduction'].insert_one(d)

# Hospitals reduced by zipcode, Intermediate collection to find hospital names by zip
hospital_zips_reduction = collectionsReduceHopsByZip(hospitalCollection)
repo.dropPermanent("hospital_zips_reduction")
repo.createPermanent("hospital_zips_reduction")
for elem in hospital_zips_reduction:
    d = {elem[0]: elem[1]}
    repo['jmuru1_joshmah_tpacius.hospital_zips_reduction'].insert_one(d)


#Hospitals by Zipcode
hospitals_by_zip = zipcodeNameAggregate(hospital_zips_reduction)
repo.dropPermanent("hospitals_by_zip")
repo.createPermanent("hospitals_by_zip")
repo['jmuru1_joshmah_tpacius.hospitals_by_zip'].insert_one(hospitals_by_zip)
# print(hospitals_by_zip)

# Sums of property values; Intermediate Collection to get avg property values
hospitals_property_sums = zipcodeAggregate(getCollection('property_zips_reduction'))
repo.dropPermanent("hospitals_property_sums")
repo.createPermanent("hospitals_property_sums")
repo['jmuru1_joshmah_tpacius.hospitals_property_sums'].insert_one(hospitals_property_sums)

# Counts of properties in specific zip code; Intermediate collection to get avg property value
hospitals_property_counts = zipcodeLengthAggregate(getCollection('property_zips_reduction'))
repo.dropPermanent("hospitals_property_counts")
repo.createPermanent("hospitals_property_counts")
repo['jmuru1_joshmah_tpacius.hospitals_property_counts'].insert_one(hospitals_property_counts)

# Average property value by zipcode
avg_property_values = propertyAvg(hospitals_property_sums,hospitals_property_counts)
repo.dropPermanent("avg_property_values")
repo.createPermanent("avg_property_values")
repo['jmuru1_joshmah_tpacius.avg_property_values'].insert_one(avg_property_values)
# print(propertyAvg(hospitals_property_sums,hospitals_property_counts))

# Hospital Name and Location by Long. and Lat.
hospital_lat_lon = extractHosptialLocations(hospitalCollection)
repo.dropPermanent("hospital_lat_lon")
repo.createPermanent("hospital_lat_lon")
repo['jmuru1_joshmah_tpacius.hospital_lat_lon'].insert_one(hospital_lat_lon)

#======================================== save to database code =====================================
# ===========================Prov==============================
endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_joshmah_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_joshmah_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:ElementaryOperations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('dat:hospital_reduction', {'prov:label':'Hospitals and Property Values Reduced By Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource2 = doc.entity('dat:hospitals_property_sums', {'prov:label':'Total Property Value by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource3 = doc.entity('dat:hospitals_property_counts', {'prov:label':'Total Property Count by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource4 = doc.entity('dat:avg_property_values', {'prov:label':'Average Property Value by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', prov.model.PROV_TYPE:'ont:Computation'})
doc.wasAssociatedWith(this_run, this_script)

doc.used(this_run, resource1, startTime)
doc.used(this_run, resource2, startTime)
doc.used(this_run, resource3, startTime)
doc.used(this_run, resource4, startTime)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
# print(doc.get_provn())
repo.logout()
# ===========================Prov End==============================
