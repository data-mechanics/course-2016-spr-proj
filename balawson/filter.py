# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, math
import prov.model
import numpy as np
exec(open('../pymongo_dm.py').read())
###############################################################
####    access the data       
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()
tweets = pd.DataFrame(list(repo.balawson.twitterIntersectionMonthCounts.find()))

users = pd.DataFrame(list(repo.balawson.twitterIntersectionMonthUsers.find()))

tweets = tweets.convert_objects(convert_numeric=True) #TODO this is getting deprenciated 
months = \
['may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'january', 'feburary', 'march', 'april']
###############################################################
####    filter the data       
###############################################################
'''
Since most of the intersections don't have many people there I filter only the intersections that have more than 5 people for EACH month
'''

#evil python/pandas voodoo
def mask(df, key, value=5):
    return df[df[key] >= value]

pd.DataFrame.mask = mask
'''
def filter_columns(df, cols, min_count=5):
    begin_str = '{0}.mask("'.format(variable_as_name(df))  
    join_str = '",{0}).mask("'.format(min_count) 
    end_str  = '",{0})'.format(min_count)
    x=  eval(begin_str + join_str.join(cols) + end_str)
    return x

def variable_as_name(var):
    for k, v in globals().items():
       if v is var and (k != 'var'):
           return k
'''
sample = tweets.mask('may').mask('june').mask('july').mask('august').mask('september').mask('october').mask('november').mask('december').mask('january').mask('feburary').mask('march').mask('april')

first_month  = []
second_month = []
users_sample = users.loc[sample.index] #get corresponding user ids
for idx, row in users_sample.iterrows():
    first_month.append(  row.june.split(',') )
    second_month.append( row.october.split(',') )

from functions import c_rp
#for each intersection calculate N
Ns = []
for idx, x in enumerate(first_month):
    Ns.append( c_rp(x, second_month[idx]))

print(Ns)
    
    


###############################################################
####    save results       
###############################################################
'''
collection_name = 'kmeans_results'
repo.dropPermanent(collection_name)
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
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

'''
# 
