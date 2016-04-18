# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# 
import datetime
import json
import prov.model
import pymongo
import re
import urllib.request
import uuid
from bson.son import SON

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
# Josh Mah helped us with this one :)
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

num_schools = int(input("Enter the number of schools you want in your zipcode: "))
num_hospitals = int(input("Enter the number of hospitals you want in your zipcode: "))
avg_tax = float(input("Enter the average tax per square foot you want as a maximum: "))

get_possible_solutions_statement = {"$and" : [ {"avg_tax_per_sf" : {"$lte" : avg_tax} },
 {"num_hospitals" : {"$gte" : num_hospitals}}, {"num_schools" : {"$gte" : num_schools}} ]}

cursor = repo['jtsliu_kmann.zipcode_profile'].find(get_possible_solutions_statement)

zipcodes = []
for elem in cursor:
	zipcodes.append(elem)


minimum_crime = 999999
safest_zip = None
for zipcode in zipcodes:
	if zipcode["num_crimes"] < minimum_crime:
		minimum_crime = zipcode["num_crimes"]
		safest_zip = zipcode

cheapest_tax = 999999
cheapest_zip = None
for zipcode in zipcodes:
	if zipcode["avg_tax_per_sf"] < cheapest_tax:
		cheapest_tax = zipcode["avg_tax_per_sf"]
		cheapest_zip = zipcode

print("The cheapest zipcode satisfying your constraints is:")
print(cheapest_zip)
print()
print("The safest zipcode satisfying your constraints is:")
print(safest_zip)

choice = input("Would you like to see all results? (y/n) ")
if choice == "y":
	for x in zipcodes:
		print(x)








