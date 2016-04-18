import matplotlib.pyplot as plt
import random
import pandas as pd
import math
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')


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
for i in repo['jlam17_mckay678.sortedFood']:
	if i['Neighborhood'] == "Allston/Brighton":
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
	elif i['Neighborhood'] == "Fenway/Kenmore":
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
	elif i['Neighborhood'] == "Hyde Park":
		if i['Type'] == 'activeFood':
			H[0] += 1
		elif i['Type'] == 'cornerStore':
			H[1] += 1
		elif i['Type'] == 'foodPantry':
			H[2] += 1
		elif i['Type'] == 'retailBakery':
			H[3] += 1
	elif i['Neighborhood'] == "Jamaica Plain":
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
	elif i['Neighborhood'] == "North Dorchester":
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
	elif i['Neighborhood'] == "South Dorchester":
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

plt.bar(index, A, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Allston/Brighton')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Allston/Brighton.png')
plt.clf

plt.bar(index, B, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Back Bay')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Back Bay.png')
plt.clf

plt.bar(index, C, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Central')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Central.png')
plt.clf

plt.bar(index, D, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Charlestown')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Charlestown.png')
plt.clf

plt.bar(index, E, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('East Boston')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('East Boston.png')
plt.clf

plt.bar(index, F, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Fenway/Kenmore')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Fenway/Kenmore.png')
plt.clf

plt.bar(index, G, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Harbor Islands')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Harbor Islands.png')
plt.clf

plt.bar(index, H, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Hyde Park')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Hyde Park.png')
plt.clf

plt.bar(index, I, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Jamaica Plain')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Jamaica Plain.png')
plt.clf

plt.bar(index, J, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Mattapan')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Mattapan.png')
plt.clf

plt.bar(index, K, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('North Dorchester')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('North Dorchester.png')
plt.clf

plt.bar(index, L, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Roslindale')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Roslindale.png')
plt.clf

plt.bar(index, M, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('Roxbury')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('Roxbury.png')
plt.clf

plt.bar(index, N, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('South Boston')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('South Boston.png')
plt.clf

plt.bar(index, O, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('South Dorchester')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('South Dorchester.png')
plt.clf

plt.bar(index, P, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('South End')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('South End.png')
plt.clf

plt.bar(index, Q, .5, alpha=opacity, color='b')
plt.xlabel('Number')
plt.ylabel('Type')
plt.title('West Roxbury')
plt.xticks(index + .5, ('Food Est.', 'Corner Store', 'Food Pantry', 'Retail Bakery'), rotation='vertical')
savefig('West Roxbury.png')
plt.clf