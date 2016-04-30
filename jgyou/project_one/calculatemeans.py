'''
calculatemeans.py

Clusters requests using k-means.

Adapted from:
http://cs-people.bu.edu/lapets/591/s.php#cba5543907854ed28dbd3eeb874ebd54

http://stackoverflow.com/questions/4270301/matplotlib-multiple-datasets-on-the-same-scatter-plot'''

from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

from bson.objectid import ObjectId
from bson.code import Code

import random
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo


f = open("auth.json").read()

auth = loads(f)
user = auth['user']
# remember to modify this line later
repo.authenticate(auth['user'], auth['user'])

startTime = datetime.datetime.now()

##########

k = 6

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

# randomly generates means using original set of points
P = [(float(point['longitude']), float(point['latitude'])) for point in repo[user + '.needle311'].find()]
M = [P[random.randint(0, len(P) - 1)] for i in range(k)]

OLD = []

while OLD != M:
    OLD = M

    MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
    PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
    PD = aggregate(PDs, min)
    MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
    MT = aggregate(MP, plus)

    M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
    MC = aggregate(M1, sum)

    M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]

repo.dropPermanent('requestsxy')
repo.dropPermanent('means')
repo.createPermanent('requestsxy')
repo.createPermanent('means')

# store cluster assignments
for xy in MP:
	for i in range(k):
		#print(M[i])
		if xy[0] == M[i]:
			lng = xy[1][0]
			lat = xy[1][1]
			repo[user + ".requestsxy"].insert({"cluster_num": i, "mean": [{"latitude": M[i][1], "longtitude": M[i][0]}], "latitude": lat, "longitude": lng}) 

# store means
for m in M:
	(x, y) = m
	repo[user + ".means"].insert({"longitude": x, "latitude": y})


# # plot in matplot
# fig = plt.figure()
# ax1 = fig.add_subplot(111)

# arrp = np.array([[a, b] for (a, b) in P])
# arrm = np.array([[a, b] for (a, b) in M])
# clusters = np.array([i for i in range(k) for xy in MP if M[i] == xy])

# ax1.scatter(arrp[:,0], arrp[:,1], c=clusters, label ="requests")
# ax1.scatter(arrm[:,0], arrm[:,1], c="k", marker="h", label="means")
# plt.legend(loc="upper left")
# plt.show()

'''
# non-mapreduce version also plots points
# code converts needle311 requests coordinates into numpy array
k = 6
p = [[float(point['longitude']), float(point['latitude'])] for point in repo[user + '.needle311'].find()]

arr = np.asarray(p)

# then plot kmeans scatterplot

results = KMeans(n_clusters=k, init='k-means++').fit_predict(arr)
plt.scatter(arr[:,0], arr[:,1], c=results)

plt.show()

repo.dropPermanent('requestsxy')
repo.createPermanent('requestsxy')

for i in arr:
	x = arr[i, 0]
	y = arr[i, 1]
 	repo[user + '.requestsxy'].insert_one({"longitude": x, "latitude": y, "label": results[i]})
'''

###########

endTime = datetime.datetime.now()

run_id = str(uuid.uuid4())

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

this_script = provdoc.agent('alg:calculatemeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('dat:needle311', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc.activity('log:a'+ run_id, startTime, endTime)
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime)

rqst = provdoc.entity('dat:means', {prov.model.PROV_LABEL:'Request Centroids', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(rqst, this_script)
provdoc.wasGeneratedBy(rqst, this_run, endTime)
provdoc.wasDerivedFrom(rqst, resource, this_run, this_run, this_run)

requestsxy = provdoc.entity('dat:requestsxy', {prov.model.PROV_LABEL:'Labeled request coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(requestsxy, this_script)
provdoc.wasGeneratedBy(requestsxy, this_run, endTime)
provdoc.wasDerivedFrom(requestsxy, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

#########
# plan

provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

this_script = provdoc2.agent('alg:calculatemeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc2.entity('dat:needle311', {prov.model.PROV_LABEL:'Needle Program', prov.model.PROV_TYPE:'ont:DataSet'})
this_run = provdoc2.activity('log:a'+ run_id, startTime, endTime)
provdoc2.wasAssociatedWith(this_run, this_script)
provdoc2.used(this_run, resource, startTime)

rqst = provdoc2.entity('dat:means', {prov.model.PROV_LABEL:'Request Centroids', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc2.wasAttributedTo(rqst, this_script)
provdoc2.wasGeneratedBy(rqst, this_run, endTime)
provdoc2.wasDerivedFrom(rqst, resource, this_run, this_run, this_run)

requestsxy = provdoc2.entity('dat:requestsxy', {prov.model.PROV_LABEL:'Labeled request coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc2.wasAttributedTo(requestsxy, this_script)
provdoc2.wasGeneratedBy(requestsxy, this_run, endTime)
provdoc2.wasDerivedFrom(requestsxy, resource, this_run, this_run, this_run)

plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)
doc2.update(provdoc2)
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
plan.close()

repo.logout()