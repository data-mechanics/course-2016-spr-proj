import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid
import apitest as apitest
# import geo as geo

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

# ========================query databse functions=================================
def getCollection(dbName):
	temp = []
	if type(dbName) != str:
		return "Error: please input a string"
	for elem in repo['jmuru1_tpacius.' + dbName].find({}):
		temp.append(elem)
	return temp
# ========================query databse functions end =================================

# ===========================Perform ops on collections==============================
zipCarReservations = getCollection("zipcarreversations")
zipCarMembers = getCollection("zipcarmembers")
propertyValues = getCollection("propertyvalue")

#reduce the property value collection onto zipcar collections
def collectionsReduce(a, b, compareCollection=propertyValues):
    x = [(memberPostal['postal_code'], memberPostal) for memberPostal in a] #zipcar members
    y = [(reservePostal['end_postal_code'], reservePostal) for reservePostal in b] #zipcar reservations
    c = [(propertyPostal['zipcode'], propertyPostal) for propertyPostal in compareCollection] #property values
    memberReduce = reduceNoFunction(x, c) #reduceNoFunction declaration within the elementary ops section
    reservationReduce = reduceNoFunction(y,c)
    return (memberReduce, reservationReduce) #return a tuple of the wo lists after reduction

def propertyAverage(propList):
    props = propList #takes in second part of each tuple (list of property)
    total = 0
    print(props)
    for elem in props:
        #increment total by value associated with this key
        total += int(elem['av_bldg']) 
        #the largest property value should have largest ratio after dividing by the size
    return (total / len(props))



(cReduceMember, cReduceReservations) = collectionsReduce(zipCarMembers, zipCarReservations) #declaration


repo.dropPermanent("membersreduction")
repo.createPermanent("membersreduction")
for elem in cReduceMember:
    d = {elem[0]: elem[1]}
    repo['jmuru1_tpacius.membersreduction'].insert_one(d)

repo.dropPermanent("reservationsreductions")
repo.createPermanent("reservationsreductions")
for elem in cReduceReservations:
    d = {elem[0]: elem[1]}
    repo['jmuru1_tpacius.reservationsreductions'].insert_one(d)

# ===========================Perform ops on collections end==============================
endTime = datetime.datetime.now()

repo.logout()

## eof
