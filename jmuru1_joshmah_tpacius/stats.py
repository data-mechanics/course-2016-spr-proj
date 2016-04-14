import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import re
import apiTest as apiTest
from math import ceil, sqrt
from random import shuffle

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jmuru1_joshmah_tpacius', 'jmuru1_joshmah_tpacius')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

# ========================query database functions=================================
def getCollection(dbName):
	temp = []
	if type(dbName) != str:
		return "Error: please input a string"
	for elem in repo['jmuru1_joshmah_tpacius.' + dbName].find({}):
		temp.append(elem)
	return temp
# ========================query database functions end =================================

# ========================Statistical ops =================================
def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)
# ===========================End stats ops ==============================


# ===========================Perform ops on collections==============================
hospitalByZip = getCollection("hospitals_by_zip")[0]
intersectHJ = getCollection("hospital_jams_count")[0]
avg_property_values = getCollection("avg_property_values")[0]

# Reduction to find number of hospitals in a zipcode
def hospitalCountByZip(collection):
	dictionary = {}
	for elem in collection.items():
		(key, value) = elem
		if(key != '_id'):
			dictionary[key] = len(value)
	return dictionary

hospitalCount = hospitalCountByZip(hospitalByZip)

# Reduction to find number of traffic jams near hospitals by zipcode
def trafficJamsByHospitalsInZip(collection1, collection2):
	dictionary = {}
	for elem in collection1.items():
		key, value = elem
		if key != '_id':
			dictionary[key] = 0
	for elem1 in collection1.items():
		zips, lst = elem1
		for elem2 in collection2.items():
			name, jams = elem2
			if isinstance(lst, list):
				if name in lst and zips != '_id' and name != '_id':
					dictionary[zips] += jams
	return dictionary

jamsByZip = trafficJamsByHospitalsInZip(hospitalByZip,intersectHJ)

# Reductions on zipcodes and average property value
def hospitalCountPropertyValue(collection1, collection2):
	dictionary = {}
	for elem1 in collection1.items():
		(key1, value1) = elem1
		for elem2 in collection2.items():
			(key2,value2) = elem2
			if key1 == key2:
				dictionary[key1] = (value1, value2)
	return dictionary

countValue = hospitalCountPropertyValue(hospitalCount, avg_property_values)
jamsValues = hospitalCountPropertyValue(jamsByZip, avg_property_values)

#calculate correlation and p values
def stats(collection):
	keys, values = collection.keys(), collection.values()
	x = [x for (x , y) in values]
	y = [y for (x , y) in values]
	return (corr(x,y), p(x,y))

# countCorr = stats(countValue)
# jamCorr = stats(jamsValues)

#=====================================End collection operations===================

#===============================Store in DB=======================================
repo.dropPermanent("hospitals_in_zip")
repo.createPermanent("hospitals_in_zip")
repo['jmuru1_joshmah_tpacius.hospitals_in_zip'].insert_one(hospitalCount)

repo.dropPermanent("trafficjam_by_zip")
repo.createPermanent("trafficjam_by_zip")
repo['jmuru1_joshmah_tpacius.trafficjam_by_zip'].insert_one(jamsByZip)

repo.dropPermanent("avg_property_hospital_count")
repo.createPermanent("avg_property_hospital_count")
repo['jmuru1_joshmah_tpacius.avg_property_hospital_count'].insert_one(countValue)

repo.dropPermanent("avg_property_trafficjams")
repo.createPermanent("avg_property_trafficjams")
repo['jmuru1_joshmah_tpacius.avg_property_trafficjams'].insert_one(jamsValues)
#===============================End Store in DB===================================

#================================Prov===========================================
endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jmuru1_joshmah_tpacius/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jmuru1_joshmah_tpacius/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:stats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('dat:hospitals_in_zip', {'prov:label':'Hospital Count by By Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource2 = doc.entity('dat:trafficjam_by_zip', {'prov:label':'Traffic Jams near Hospitals by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource3 = doc.entity('dat:avg_property_hospital_count', {'prov:label':'Average Property Value and Hospital Count by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
resource4 = doc.entity('dat:avg_property_trafficjams', {'prov:label':'Average Property Value and Traffic Jams near Hospital by Zipcode', prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
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
#================================End Prov===========================================

#eof