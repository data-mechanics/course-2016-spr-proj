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
months = ['may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'january', 'feburary', 'march', 'april']
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

sample['estimate'] = Ns
sample.drop(months, inplace=True, axis=1)
sample.drop('_id', inplace=True, axis=1)
    

###############################################################
####    plot results
###############################################################
import folium
outputname = 'estimate.html'
map_osm = folium.Map(location=[42.355,-71.0609], zoom_start=13)
for idx, row in sample.iterrows():
    lat, lng = row.lat, row.lng
    if row.estimate > 0:
        ratio = math.log2(row.estimate*2)
        map_osm.polygon_marker(location=[lat, lng], popup='estimate: '+ str(row.estimate),fill_color='#ff0000', num_sides=8, radius=ratio, fill_opacity=0.3) 
    else:
        ratio = 10
        map_osm.polygon_marker(location=[lat, lng], popup=' estimate: '+ str(row.estimate),fill_color='#0000ff', num_sides=4, radius=ratio, fill_opacity=0.3) 
map_osm.create_map(path=outputname)

###############################################################
####    save results       
###############################################################
records = json.loads(sample.T.to_json()).values()
print(type(records))
collection_name = 'capture_recapture'
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
doc.add_namespace('bal', 'http://people.bu.edu/balawson/')

this_script = doc.agent('alg:estimate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

act_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(act_twitter, this_script)

doc.used(act_twitter, twitter_resource, startTime)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
# 
