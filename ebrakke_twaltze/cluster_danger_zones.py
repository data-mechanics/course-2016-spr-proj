import dml
import prov.model
import datetime
import uuid
import numpy as np
import scipy as sp
import pandas
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sklearn.cluster as cluster
from sklearn.preprocessing import StandardScaler

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ebrakke_twaltze', 'ebrakke_twaltze')

def buildGraph(df, clusters, fName = 'kmeans.png'):
	colors = np.array([x for x in 'bgrcmykbgrcmykbgrcmykbgrcmyk'])
	colors = np.hstack([colors] * len(clusters))
	plt.scatter(df[['lat']], df[['lng']], color=colors[clusters].tolist(), s=10, alpha=0.8)
	plt.savefig(fName)

def findBestCluster(dataset, maxClusters = 100):
	df = pandas.DataFrame(list(dataset))
	data = df[['lat', 'lng']].copy()
	data.columns = ['lat', 'lng']

	clusters = None
	centers = None
	lowestError = None
	for k in range(2, maxClusters):
		print(str(k) + ' clusters')

		kmeans = cluster.KMeans(n_clusters = k)
		kmeans.fit(data)
		prediction = kmeans.labels_.astype(np.int)
		error = kmeans.inertia_

		if not lowestError:
			lowestError = error
			clusters = prediction
		else:
			improvement = (lowestError - error) / lowestError

			print('lowestError\t' + str(lowestError))
			print('improvement\t' +str(improvement))

			if improvement < 0:
				pass
			elif improvement <= .02:
				return (df, prediction, kmeans.cluster_centers_)
			else:
				lowestError = error
				clusters = prediction
				centers = kmeans.cluster_centers_

		print('-----------------------')

	# Never converged, but return the last clustering
	return (df, clusters, centers)

def kmeans():
	start_time = datetime.datetime.now()
	dataset = repo['ebrakke_twaltze.dangerLevels'].find({'dangerLevel': {"$gt": 0}})

	data, clusters, centers = findBestCluster(dataset, maxClusters = 50)
	data['cluster'] = clusters
	data.drop('_id', axis = 1, inplace = True)

	# Add a cluster attribute to each dangerLevel
	repo['ebrakke_twaltze.dangerLevels'].remove({})
	repo['ebrakke_twaltze.dangerLevels'].insert_many(data.to_dict('records'))

	# Store the center of each calculated cluster
	collection = 'dangerZones'
	repo.dropPermanent(collection)
	repo.createPermanent(collection)

	centers = pandas.DataFrame(centers)
	centers.columns = ['lat', 'lng']
	centers['cluster'] = centers.index
	repo['ebrakke_twaltze.' + collection].insert_many(centers.to_dict('records'))

	buildGraph(data, clusters)
	end_time = datetime.datetime.now()
	do_prov(start_time, end_time)

def do_prov(start_time, end_time):
	doc = prov.model.ProvDocument()
	doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ebrakke_twaltze/')
	doc.add_namespace('dat', 'http://datamechanics.io/data/ebrakke_twaltze/')
	doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
	doc.add_namespace('log', 'http://datamechanics.io/log#')

	script = doc.agent('alg:cluster_danger_zones', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
	danger_levels = doc.entity('dat:dangerLevels', {prov.model.PROV_LABEL: 'Danger Levels', prov.model.PROV_TYPE:'ont:DataSet'})

	cluster = doc.activity('log:a' + str(uuid.uuid4()), start_time, end_time, {prov.model.PROV_LABEL: 'Cluster the danger zones into means'})
	doc.wasAssociatedWith(cluster, script)
	doc.usage(cluster, danger_levels, start_time, None, {prov.model.PROV_TYPE:'ont:Computation'})

	danger_zones = doc.entity('dat:dangerZones', {prov.model.PROV_LABEL: 'Danger Zones', prov.model.PROV_TYPE:'ont:DataSet'})
	doc.wasAttributedTo(danger_zones, script)
	doc.wasGeneratedBy(danger_zones, cluster, end_time)
	doc.wasDerivedFrom(danger_zones, danger_levels, cluster, cluster, cluster)

	repo.record(doc.serialize())
	print(doc.get_provn())
    
if __name__ == '__main__':
	kmeans()
