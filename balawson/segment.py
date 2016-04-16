# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, math, os, subprocess
exec(open('../pymongo_dm.py').read())

###############################################################
####   define context manager
####   from stack overflow: http://stackoverflow.com/questions/431684/how-do-i-cd-in-python
###############################################################
class cd:
    """Context manager for changing the current working directory"""
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)

    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)
###############################################################
####    access the data       
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()
tweets     = pd.DataFrame(list(repo.balawson.twitter.find()))
gowalla    = pd.DataFrame(list(repo.balawson.gowalla.find()))
brightkite = pd.DataFrame(list(repo.balawson.brightkite.find()))

tweets.time = tweets.time.apply(lambda d: pd.to_datetime(d*1000000))

###############################################################
####    group the data by time of day
###############################################################

groups = tweets.groupby(tweets.time.map(lambda t: str(t.time)))
#for group in groups:
#save temp file
#run the following (with temp file as input)
'''
    with cd("./BostonNetwork/src/"):
        print (os.getcwd())
        os.popen("javac getintersections/RoadNetwork.java;")
        time.sleep(4)
        pipe = os.popen("java  getintersections.RoadNetwork")
        print pipe.readlines()
'''
#call xml to dataframe 
#move the temp file output to something else or load in python memory
#save all to mongo

###############################################################
####    save results       
###############################################################

records = {'labels'  : data_labels,\
     'centers' : data_cluster_centers,\
     'size'    :  data_num_each_cluster,\
     'date_created' : datetime.datetime.today(),
     'k'        : k
          }
  
collection_name = 'kmeans_results'
# doing this because kmeans is random and we may want different results
if not(db[collection_name]):
    repo.createPermanent(collection_name)
#repo.dropPermanent(collection_name)

repo['balawson.' + collection_name].insert(records)

endTime  = datetime.datetime.now()

###############################################################
####    record provanence       
###############################################################
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/balawson/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/balawson/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('snap', 'https://snap.stanford.edu/data/')
doc.add_namespace('bal', 'http://people.bu.edu/balawson/')

this_script = doc.agent('alg:cluster', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
brightkite_resource = doc.entity('snap:brightkite', {'prov:label':'SNAP: Standford Network Analysis Project - Brightkite', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
gowalla_resource = doc.entity('snap:gowalla', {'prov:label':'SNAP: Standford Network Analysis Project - Gowalla', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

viz_brightkite = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
viz_gowalla = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
viz_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(viz_brightkite, this_script)
doc.wasAssociatedWith(viz_gowalla, this_script)
doc.wasAssociatedWith(viz_twitter, this_script)

doc.used(viz_brightkite, brightkite_resource, startTime)
doc.used(viz_gowalla, gowalla_resource, startTime)
doc.used(viz_twitter, twitter_resource, startTime)

brightkite_ent = doc.entity('dat:brightkite', {prov.model.PROV_LABEL:'Brightkite data', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(brightkite_ent, this_script)
doc.wasDerivedFrom(brightkite_ent, brightkite_resource)

gowalla_ent = doc.entity('dat:gowalla', {prov.model.PROV_LABEL:'Gowalla dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(gowalla_ent, this_script)
doc.wasDerivedFrom(gowalla_ent, gowalla_resource)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


# 
