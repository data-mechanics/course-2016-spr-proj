# coding: utf-8
###############################################################
####   import dependancies       
###############################################################

import pandas as pd
import pymongo, datetime, uuid
import prov.model
import random
from scic_stat_tests import *
exec(open('../pymongo_dm.py').read())

##############################################################
####   random sampler
#stackoverflow.com/questions/6482889/get-random-sample-from-list-while-maintaining-ordering-of-items
##############################################################
def orderedSampleWithoutReplacement(seq, k):
    if not 0<=k<=len(seq):
        raise ValueError('Required that 0 <= sample_size <= population_size')

    numbersPicked = 0
    for i,number in enumerate(seq):
        prob = (k-numbersPicked)/(len(seq)-i)
        if random.random() < prob:
            yield number
            numbersPicked += 1
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

####create random samples of each dataset (because they are too big)
b_test =  brightkite.iloc[list(orderedSampleWithoutReplacement([x for x in range(0,len(brightkite.lat))], 5000))]
g_test =  gowalla.iloc[list(orderedSampleWithoutReplacement([x for x in range(0,len(gowalla.lat))], 5000))]
t_test =  tweets.iloc[list(orderedSampleWithoutReplacement([x for x in range(0,len(tweets.lat))], 5000))]

###############################################################
####    analyze the data       
###############################################################

###### test similarity between geocoordinates in datasets based on random samples
print('calculating two sample, two dimensional (geocoordinates) ,kolmogoro-smirnov tests...')
print('\tbetween Gowalla and Twitter: ',)
g_t_results = (ks2d2s(list(g_test.lat), list(g_test.lng), list(t_test.lat), list(t_test.lng)))
print('\tbetween Brightkite and Twitter: ',)
b_t_results = (ks2d2s(list(b_test.lat), list(b_test.lng), list(t_test.lat), list(t_test.lng)))
print('\tbetween Gowalla and Brightkite: ',)
g_b_results = (ks2d2s(list(g_test.lat), list(g_test.lng), list(b_test.lat), list(b_test.lng)))


'''
TODO: put the correct time collumn in (this compares between milisecond differences not hour/min differences
###### test similarity between timestamps in datasets based on random samples
print('calculating two sample, one dimensional (time),kolmogoro-smirnov tests...')
print('\tbetween Gowalla and Twitter: ',)
print(ks2d2s(list(g_test.time), list(t_test.time)))
print('\tbetween Brightkite and Twitter: ',)
print(ks2d2s(list(b_test.time), list(t_test.time)))
print('\tbetween Gowalla and Brightkite: ',)
print(ks2d2s(list(g_test.time), list(t_test.time)))
'''
###############################################################
####    save results       
###############################################################

records = {'gowalla_twitter' : g_t_results,
           'brightkite_twitter' : b_t_results,
           'gowalla_brightkite': g_b_results,
     'date_created' : datetime.datetime.today()\
          }
collection_name = 'ks_test_results'
# doing this because kmeans is random and we may want different results
if not(repo[collection_name]):
    repo.createPermanent(collection_name)
#repo.dropPermanent(collection_name)
repo['balawson.' + collection_name].insert(records)
endTime = datetime.datetime.now()
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

this_script = doc.agent('alg:compare', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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
