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
# hospitalByZip = getCollection("hospitals_by_zip")[0]
hospitalByZip = getCollection("hospitals_by_zip2")[0]
intersectHJ = getCollection("hospital_jams_count")[0]
# avg_property_values = getCollection("avg_property_values")[0]
avg_property_values = getCollection("avg_property_values2")[0]
# print(hospitalByZip)
# print(intersectHJ)
# print(avg_property_values)

def hospitalCountByZip(collection):
	dictionary = {}
	for elem in collection.items():
		(key, value) = elem
		if(key != '_id'):
			dictionary[key] = len(value)
	return dictionary

hospitalCount = hospitalCountByZip(hospitalByZip)

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
	# print(dictionary)
	return dictionary

jamsByZip = trafficJamsByHospitalsInZip(hospitalByZip,intersectHJ)
			
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
print(countValue)
jamsValues = hospitalCountPropertyValue(jamsByZip, avg_property_values)
print(jamsValues)
print()

def stats(collection):
	keys, values = collection.keys(), collection.values()
	x = [x for (x , y) in values]
	y = [y for (x , y) in values]
	print("Covariance = " + str(cov(x,y)))
	print("Correlation coefficient = " + str(corr(x,y)))
	print("p value = " + str(p(x,y)))
	return

print("Property Values and Hospital Count Stats")
stats(countValue)
print()
print("Property Values and Jams By Hospital Stats")
stats(jamsValues)







