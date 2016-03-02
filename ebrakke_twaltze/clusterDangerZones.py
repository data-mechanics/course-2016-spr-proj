import pymongo

import numpy as np
import scipy as sp
import pandas

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import sklearn.cluster as cluster
from sklearn.preprocessing import StandardScaler

exec(open('../pymongo_dm.py').read())

def buildGraph(df, clusters, fName = 'kmeans.png'):
	colors = np.array([x for x in 'bgrcmykbgrcmykbgrcmykbgrcmyk'])
	colors = np.hstack([colors] * len(clusters))
	plt.scatter(df[['lat']], df[['lng']], color=colors[clusters].tolist(), s=10, alpha=0.8)
	plt.savefig(fName)
 
def findBestCluster(dataset, maxClusters = 100):
	df = pandas.DataFrame(list(dataset));
	data = df[['lat', 'lng']]
	n = len(data)

	clusters = None
	lowestError = 1
	for k in range(2, maxClusters):
		print(k)
		kmeans = cluster.KMeans(n_clusters = k)
		prediction = kmeans.fit_predict(data)
		error = kmeans.inertia_

		improvement = lowestError - error / lowestError
		print(improvement)
		print('lowestError')
		print(lowestError)
		
		if (improvement > .05):
			lowestError = error
			clusters = prediction
		else:
			print(kmeans.cluster_centers_)
			return (df, prediction)

	# Never converged, but return the last clustering
	return (df, clusters)

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

dataset = repo['ebrakke_twaltze.dangerLevels'].find({'dangerLevel': {"$gt": 0}})

data, clusters = findBestCluster(dataset, maxClusters = 50)
# buildGraph(data, prediction)
