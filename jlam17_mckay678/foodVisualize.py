import matplotlib.pyplot as plt
import random
import pandas as pd
import math
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
from pymongo import MongoClient

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')

filen = 'fixedFood.json'
res = open(filen, 'r')
r = json.load(res)

#repo['jlam17_mckay678.fixedFood']
A = [0,0,0,0]
B = [0,0,0,0]
C = [0,0,0,0]
D = [0,0,0,0]
E = [0,0,0,0]
F = [0,0,0,0]
G = [0,0,0,0]
H = [0,0,0,0]
I = [0,0,0,0]
J = [0,0,0,0]
K = [0,0,0,0]
L = [0,0,0,0]
M = [0,0,0,0]
N = [0,0,0,0]
O = [0,0,0,0]
P = [0,0,0,0]
Q = [0,0,0,0]
avg = [0,0,0,0]


for i in r:
	if i['Type'] == 'activeFood':
		avg[0] += 1
	elif i['Type'] == 'cornerStore':
		avg[1] += 1
	elif i['Type'] == 'foodPantry':
		avg[2] += 1
	elif i['Type'] == 'retailBakery':
		avg[3] += 1
	if (i['Neighborhood'] == "Allston") or (i['Neighborhood'] == "Brighton"):
		if i['Type'] == 'activeFood':
			A[0] += 1
		elif i['Type'] == 'cornerStore':
			A[1] += 1
		elif i['Type'] == 'foodPantry':
			A[2] += 1
		elif i['Type'] == 'retailBakery':
			A[3] += 1
	elif i['Neighborhood'] == "Back Bay":
		if i['Type'] == 'activeFood':
			B[0] += 1
		elif i['Type'] == 'cornerStore':
			B[1] += 1
		elif i['Type'] == 'foodPantry':
			B[2] += 1
		elif i['Type'] == 'retailBakery':
			B[3] += 1
	elif i['Neighborhood'] == "Central":
		if i['Type'] == 'activeFood':
			C[0] += 1
		elif i['Type'] == 'cornerStore':
			C[1] += 1
		elif i['Type'] == 'foodPantry':
			C[2] += 1
		elif i['Type'] == 'retailBakery':
			C[3] += 1
	elif i['Neighborhood'] == "Charlestown":
		if i['Type'] == 'activeFood':
			D[0] += 1
		elif i['Type'] == 'cornerStore':
			D[1] += 1
		elif i['Type'] == 'foodPantry':
			D[2] += 1
		elif i['Type'] == 'retailBakery':
			D[3] += 1
	elif i['Neighborhood'] == "East Boston":
		if i['Type'] == 'activeFood':
			E[0] += 1
		elif i['Type'] == 'cornerStore':
			E[1] += 1
		elif i['Type'] == 'foodPantry':
			E[2] += 1
		elif i['Type'] == 'retailBakery':
			E[3] += 1
	elif (i['Neighborhood'] == "Fenway/Kenmore") or (i['Neighborhood'] == "Boston/Fenway") or (i['Neighborhood'] == "Fenway/") or (i['Neighborhood'] == "Fenway"):
		if i['Type'] == 'activeFood':
			F[0] += 1
		elif i['Type'] == 'cornerStore':
			F[1] += 1
		elif i['Type'] == 'foodPantry':
			F[2] += 1
		elif i['Type'] == 'retailBakery':
			F[3] += 1
	elif i['Neighborhood'] == "Harbor Islands":
		if i['Type'] == 'activeFood':
			G[0] += 1
		elif i['Type'] == 'cornerStore':
			G[1] += 1
		elif i['Type'] == 'foodPantry':
			G[2] += 1
		elif i['Type'] == 'retailBakery':
			G[3] += 1
	elif (i['Neighborhood'] == "Hyde Park") or (i['Neighborhood'] == "Hyde"):
		if i['Type'] == 'activeFood':
			H[0] += 1
		elif i['Type'] == 'cornerStore':
			H[1] += 1
		elif i['Type'] == 'foodPantry':
			H[2] += 1
		elif i['Type'] == 'retailBakery':
			H[3] += 1
	elif (i['Neighborhood'] == "Jamaica Plain") or (i['Neighborhood'] == "Jamaica"):
		if i['Type'] == 'activeFood':
			I[0] += 1
		elif i['Type'] == 'cornerStore':
			I[1] += 1
		elif i['Type'] == 'foodPantry':
			I[2] += 1
		elif i['Type'] == 'retailBakery':
			I[3] += 1
	elif i['Neighborhood'] == "Mattapan":
		if i['Type'] == 'activeFood':
			J[0] += 1
		elif i['Type'] == 'cornerStore':
			J[1] += 1
		elif i['Type'] == 'foodPantry':
			J[2] += 1
		elif i['Type'] == 'retailBakery':
			J[3] += 1
	elif (i['Neighborhood'] == "North Dorchester") or (i['Neighborhood'] == "South Dorchester") or (i['Neighborhood'] == "Dorchester"):
		if i['Type'] == 'activeFood':
			K[0] += 1
		elif i['Type'] == 'cornerStore':
			K[1] += 1
		elif i['Type'] == 'foodPantry':
			K[2] += 1
		elif i['Type'] == 'retailBakery':
			K[3] += 1
	elif i['Neighborhood'] == "Roslindale":
		if i['Type'] == 'activeFood':
			L[0] += 1
		elif i['Type'] == 'cornerStore':
			L[1] += 1
		elif i['Type'] == 'foodPantry':
			L[2] += 1
		elif i['Type'] == 'retailBakery':
			L[3] += 1
	elif i['Neighborhood'] == "Roxbury":
		if i['Type'] == 'activeFood':
			M[0] += 1
		elif i['Type'] == 'cornerStore':
			M[1] += 1
		elif i['Type'] == 'foodPantry':
			M[2] += 1
		elif i['Type'] == 'retailBakery':
			M[3] += 1
	elif i['Neighborhood'] == "South Boston":
		if i['Type'] == 'activeFood':
			N[0] += 1
		elif i['Type'] == 'cornerStore':
			N[1] += 1
		elif i['Type'] == 'foodPantry':
			N[2] += 1
		elif i['Type'] == 'retailBakery':
			N[3] += 1
	elif i['Neighborhood'] == "Boston":
		if i['Type'] == 'activeFood':
			O[0] += 1
		elif i['Type'] == 'cornerStore':
			O[1] += 1
		elif i['Type'] == 'foodPantry':
			O[2] += 1
		elif i['Type'] == 'retailBakery':
			O[3] += 1
	elif i['Neighborhood'] == "South End":
		if i['Type'] == 'activeFood':
			P[0] += 1
		elif i['Type'] == 'cornerStore':
			P[1] += 1
		elif i['Type'] == 'foodPantry':
			P[2] += 1
		elif i['Type'] == 'retailBakery':
			P[3] += 1
	elif i['Neighborhood'] == "West Roxbury":
		if i['Type'] == 'activeFood':
			Q[0] += 1
		elif i['Type'] == 'cornerStore':
			Q[1] += 1
		elif i['Type'] == 'foodPantry':
			Q[2] += 1
		elif i['Type'] == 'retailBakery':
			Q[3] += 1

index = np.arange(4)

print(A)
print(B)
print(C)
print(D)
print(E)
print(F)
print(G)
print(H)
print(I)
print(J)
print(K)
print(L)
print(M)
print(N)
print(O)
print(P)
print(Q)

for idx, i in enumerate(avg):
	avg[idx] = i/17

plt.bar(index, avg, .5, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Average')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/average', bbox_inches='tight', pad_inches=.2)
plt.close()


#graph the neighborhoods and save them as pngs
plt.bar(index, A, .5, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Allston/Brighton')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/allstonBrighton', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, B, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Back Bay')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/backBay', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, C, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Central')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/central', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, D, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Charlestown')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/charlestown', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, E, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('East Boston')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/eastBoston', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, F, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Fenway/Kenmore')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/fenwayKenmore', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, G, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Harbor Islands')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/harborIslands', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, H, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Hyde Park')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/hydePark', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, I, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Jamaica Plain')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/jamaicaPlain', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, J, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Mattapan')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/mattapan', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, K, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Dorchester')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/dorchester', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, L, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Roslindale')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/roslindale', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, M, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Roxbury')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/roxbury', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, N, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('South Boston')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/southBoston', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, O, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Boston')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/boston', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, P, .5,  color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('South End')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/southEnd', bbox_inches='tight', pad_inches=.2)
plt.close()

plt.bar(index, Q, .5, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('West Roxbury')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
plt.savefig('visual/westRoxbury', bbox_inches='tight', pad_inches=.2)
plt.close()

