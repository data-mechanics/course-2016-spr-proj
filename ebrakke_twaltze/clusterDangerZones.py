import pymongo

import numpy as np
import scipy as sp
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sklearn.cluster as cluster
from sklearn.preprocessing import StandardScaler

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

dataset = repo['ebrakke_twaltze.dangerLevels'].find({'dangerLevel': {"$gt": 0}}).limit(50000)

data = []
for d in dataset:
    data.append([d.get('lat'), d.get('lng')])

data = StandardScaler().fit_transform(data)

# dbscan = cluster.DBSCAN(eps=.3)
# dbscan.fit(data)
#
# y_pred = dbscan.labels_.astype(np.int)
#
colors = np.array([x for x in 'bgrcmykbgrcmykbgrcmykbgrcmyk'])
colors = np.hstack([colors] * 20)
# plt.scatter(data[:, 0], data[:, 1], color=colors[y_pred].tolist(), s=10, alpha=0.8)
# plt.savefig('dbscan.png')



kmeans = cluster.KMeans(n_clusters=50)
kmeans.fit(data)

y_pred = kmeans.labels_.astype(np.int)

plt.scatter(data[:, 0], data[:, 1], color=colors[y_pred].tolist(), s=10, alpha=0.8)
plt.savefig('kmeans.png')
