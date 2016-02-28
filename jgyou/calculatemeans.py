'''
http://stackoverflow.com/questions/4973095/mongodb-how-to-change-the-type-of-a-field
http://stackoverflow.com/questions/25983228/convert-a-string-to-a-number-in-mongodb-projection
'''

from urllib import parse, request
from json import loads, dumps

import pymongo
import prov.model
import datetime
import uuid

import random
from sklearn.cluster import KMeans
import numpy as np
import matplotlib.pyplot as plt

exec(open('../pymongo_dm.py').read())

def distance(u, v):
	return (u['longitude'] - v['longitude'])**2 + (u['latitude'] - v['latitude'])**2

client = pymongo.MongoClient()
repo = client.repo


f = open("auth.json").read()

auth = loads(f)
user = auth['user']
# remember to modify this line later
repo.authenticate(auth['user'], auth['user'])

startTime = datetime.datetime.now()

##########

# # store coordinates in 
# repo.dropPermanent('xy')
# repo.dropPermanent('means')
# repo.createPermanent('xy')
# repo.createPermanent('means')


k = 6
p = [[float(point['longitude']), float(point['latitude'])] for point in repo[user + '.needle311'].find()]

arr = np.asarray(p)

results = KMeans(n_clusters=k, init='k-means++').fit_predict(arr)
plt.scatter(arr[:,0], arr[:,1], c=results)

plt.show()

#p_size = len(p)
#m = [p[random.randint(0, p_size - 1)] for i in range(8)]




###########

endTime = datetime.datetime.now()

repo.logout()