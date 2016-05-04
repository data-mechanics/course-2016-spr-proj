# Kyle Mann and Jonathan Liu (jtsliu_kmann)
# CS591
# solve_optimal_zipcode.py
# This file solves our super cool optimization problem. Using z3, we find an optimal subset
# of zipcodes that maximizes one parameter and minimizes the other.
# NOTE: if you only want a max/min, this is just a query and you can use query_zipcodes.py
import datetime
import json
import prov.model
import pymongo
import re
import uuid
import z3

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
# Josh Mah helped us with this one :)
# we just dont want to divide by 0
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({ "avg_tax_per_sf" : {"$gt" : 0} }):
		temp.append(elem)
	return temp

zipcode_information = getCollection("zipcode_profile")

# Read in user input - while loops for validation
number_zipcodes = len(zipcode_information) + 1
while (number_zipcodes > len(zipcode_information)):
	print("Please enter a number less than " + str(len(zipcode_information)))
	number_zipcodes = int(input("Enter a maximum number of zipcodes you are interested in: " ))
print("Choices: 'num_schools', 'avg_tax_per_sf', 'num_hospitals', 'number_properties', 'num_crimes', 'liquor_locations'")
print("Please input as strings if running in python 2")

maximize_this = "banana"
while(not maximize_this in ['num_schools', 'avg_tax_per_sf', 'num_hospitals', 'number_properties', 'num_crimes', 'liquor_locations']):
	maximize_this = input("Enter a parameter to maximize (MAX) from the above choices: ")

minimize_this = "oreo"
while(not minimize_this in ['num_schools', 'avg_tax_per_sf', 'num_hospitals', 'number_properties', 'num_crimes', 'liquor_locations']):
	minimize_this = input("Enter a parameter to minimize (MIN) from the above choices: ")


# Initialize all the zipcodes as z3 Ints
X = [ z3.Int('x' + str(i)) for i in range(len(zipcode_information)) ]

maximized_parameter_vector = []
minimized_parameter_vector = []
mapping_int_to_zip = {}
val = 0

# Pull out the data from the collection
for elem in zipcode_information:
	mapping_int_to_zip[val] = elem["_id"]
	maximized_parameter_vector.append(elem[maximize_this])
	minimized_parameter_vector.append(elem[minimize_this])
	val += 1

S = z3.Optimize()

# Now we add constraints:

# Make sure we only select a subset
S.add(z3.Sum(X) > 0)
S.add(z3.Sum(X) <= number_zipcodes)

# Make sure they are 0 or 1
for x in X:
	S.add(x >= 0)
	S.add(x <= 1)

Y =[]
Z =[]
val = 0
# Use the lists from before to build up what the value of a solution would be
for x in X:
	y = z3.Real('y' + str(val))
	z = z3.Real('z' + str(val))
	Y.append(y)
	Z.append(z)
	S.add(y == z3.ToReal(x) * maximized_parameter_vector[val])
	S.add(z == z3.ToReal(x) * minimized_parameter_vector[val])
	val += 1

# Set up the min and max
S.maximize(z3.Sum(Y))
S.minimize(z3.Sum(Z))

# Get a solution
S.check()
solution = S.model()

print("Here are the zipcodes you may be interested in:")

val = 0
solution_zips = []
for x in X:
	if val == len(zipcode_information):
		break
	if solution[x].as_long() == 1:
		print(mapping_int_to_zip[val])
		solution_zips.append(mapping_int_to_zip[val])
	val += 1

print("\n")

print("Here is them in detail\n")
for sol in solution_zips:
	print(list(repo['jtsliu_kmann.zipcode_profile'].find({'_id' : sol}))[0])
	print('\n')






