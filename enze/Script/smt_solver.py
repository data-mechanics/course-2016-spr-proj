'''
Script for SMT Optimize Solver
Used for the problem on Part 4.1

Enze Yan
enze@bu.edu
'''

import time
import csv
from z3 import *

# create a dictionary contains all the street name variables
street = {}

# create a list of list contains all the intersections
intersects = []

# track increament variable
i = 0

# Optimize API solver objective functions
opt = Optimize()

# adding integer variables
X = []

# adding constrain for each street variable
C = []

# read from the .csv file
with open("../Dataset/intersection_lane.csv") as f:
	reader = csv.reader(f, delimiter=",")
	# for each row in file:
	for row in reader:
		st1 = row[0]
		# create two new variable STREET1, STREET2
		if not st1 in street:
			s = "x" + str(i)
			street[st1] = s
			# adding integer variables
			st = Int(s)
			X.append( st )
			# adding constrain
			opt.add( Or( st = 2, st = -1 ) )
			i = i + 1
		st2 = row[1]
		if not st2 in street:
			s = "x" + str(i)
			street[st2] = s
			# adding integer variables
			st = Int(s)
			X.append( st )
			# adding constrain
			C.append( Or(st = 2, st = -1) )
			i = i + 1

		# add intersection information
		# if 1HASLANE == 0 and 2HASLANE == 0
		if (row[2] == "0.0") and (row[3] == "0.0"):
			l = [street[st1], street[st2]]
			l1 = [street[st2], street[st1]]
			if (not l in intersects) or (not l1 in intersects):
				intersects.append(l)
		# if 1HASLANE == 0 and 2HASLANE == 1
		if (row[2] == "0.0") and (row[3] == "1.0"):
			l = [2, street[st1]]
			if not l in intersects:
				intersects.append(l)
		# if 1HASLANE == 1 and 2HASLANE == 0
		if (row[2] == "1.0") and (row[3] == "0.0"):
			l = [2, street[st2]]
			if not l in intersects:
				intersects.append(l)

# objective function
O = [ Int(i)*Int(j) - 1 for (i, j) in intersects ]

# constant indication how many variables wanted to change
k = 10
opt.add( Sum(X) <= 2*k )

# print(opt.check())
# print(opt.model())

print("Begin Optimization: Maximize")
# Calculate running time
start_time = time.time()

solution = opt.maximize( And(O) )

print("--- %s seconds ---" % (time.time() - start_time))

if opt.check() == sat:
	while opt.check() == sat:
		print(solution.value())
else:
	print("Failed to solve.")

m = opt.model()
for x in street:
	print( m.evaluate(x) )
