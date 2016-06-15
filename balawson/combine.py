# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import dml, datetime, uuid, math
import prov.model
###############################################################
####    access the data       
###############################################################
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()

###############################################################
####    combine the intersection data by hour
###############################################################
##this should not be hard coded 
collectionnames = \
['bigtwitterdate2015-05-31 00:00:00',
 'bigtwitterdate2015-06-30 00:00:00',
 'bigtwitterdate2015-07-31 00:00:00',
 'bigtwitterdate2015-08-31 00:00:00',
 'bigtwitterdate2015-09-30 00:00:00',
 'bigtwitterdate2015-10-31 00:00:00',
 'bigtwitterdate2015-11-30 00:00:00',
 'bigtwitterdate2015-12-31 00:00:00',
 'bigtwitterdate2016-01-31 00:00:00',
 'bigtwitterdate2016-02-29 00:00:00',
 'bigtwitterdate2016-03-31 00:00:00',
 'bigtwitterdate2016-04-30 00:00:00']
months = ['may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'january', 'feburary', 'march', 'april']
populations = {}
users = {}
for idx, x in enumerate(collectionnames):
    p = pd.DataFrame(list(eval("repo.balawson['{0}'].find()".format(x))))
    populations.update({'{0}'.format(months[idx]) : p.population})
    users.update({'{0}'.format(months[idx]) : p.users})

#populationLists = []
populationFrame = pd.DataFrame(populations)
#for idx, row in populationFrame.iterrows():
#    populationLists.append( ([[(populationFrame.columns[id]), i] for id, i in enumerate(row)]) )

#userLists = []
userFrame = pd.DataFrame(users)
#for idx, row in userFrame.iterrows():
#    userLists.append( ([[(userFrame.columns[id]), i] for id, i in enumerate(row)]) )

populationFrame['lat'] = userFrame['lat'] = p.lat
populationFrame['lng'] = userFrame['lng'] = p.lng
populationFrame['key'] = userFrame['key'] = p.apply(lambda d: str(d.lat) + str(d.lng), axis=1 )

collection_name = 'twitterIntersectionMonthCounts'
records = json.loads(populationFrame.T.to_json()).values()
repo.dropPermanent(collection_name)
repo.createPermanent(collection_name)
repo['balawson.' + collection_name].insert_many(records)

collection_name = 'twitterIntersectionMonthUsers'
records = json.loads(userFrame.T.to_json()).values()
repo.dropPermanent(collection_name)
repo.createPermanent(collection_name)
repo['balawson.' + collection_name].insert_many(records)
'''
final_json = []
for idx, row in p.iterrows():
    name       = row.roads
    regions    = "None"
    lng        = [[x, row.lng] for x in range(0,24)]
    lat        = [[x, row.lat] for x in range(0,24)]
    population = populationLists[idx] 
    final_json += [{
    'name' : name, \
    'region' : regions,\
    'lng'   : lng,\
    'lat'   : lat,\
    'population' : population\
    }] 


###############################################################
####    save results  (was going to be used for d3 - but didn't work out
###############################################################

json.dump(final_json, open('d3/temp.json', 'w'))
'''
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

this_script = doc.agent('alg:combine', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

comp_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(comp_twitter, this_script)

doc.used(comp_twitter, twitter_resource, startTime)


twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter population dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


# 
