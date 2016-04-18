# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, math
import prov.model
exec(open('../pymongo_dm.py').read())
###############################################################
####    access the data       
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()
#tweets     = pd.DataFrame(list(repo.balawson.twitter.find()))
#gowalla    = pd.DataFrame(list(repo.balawson.gowalla.find()))
#brightkite = pd.DataFrame(list(repo.balawson.brightkite.find()))

#df = pd.concat([tweets, brightkite, gowalla])
###############################################################
####    combine the intersection data by hour
###############################################################
populations = {}
for x in range(0,24):
    p = pd.DataFrame(list(eval("repo.balawson.twitterhour{0}.find()".format(x))))
    populations.update({'{0}'.format(x) : p.population})

populationLists = []
populationFrame = pd.DataFrame(populations)
for idx, row in populationFrame.iterrows():
    populationLists.append( ([[int(populationFrame.columns[id]), int(i)] for id, i in enumerate(row)]) )

populationFrame['lat'] = p.lat
populationFrame['lng'] = p.lng
populationFrame['key'] = p.apply(lambda d: str(d.lat) + str(d.lng), axis=1 )

collection_name = 'twitterIntersectionHours'
records = json.loads(populationFrame.T.to_json()).values()
repo.dropPermanent(collection_name)
repo.createPermanent(collection_name)
repo['balawson.' + collection_name].insert_many(records)

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
####    save results       
###############################################################

json.dump(final_json, open('d3/temp.json', 'w'))

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

this_script = doc.agent('alg:combine', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#brightkite_resource = doc.entity('snap:brightkite', {'prov:label':'SNAP: Standford Network Analysis Project - Brightkite', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
#gowalla_resource = doc.entity('snap:gowalla', {'prov:label':'SNAP: Standford Network Analysis Project - Gowalla', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

#viz_brightkite = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
#viz_gowalla = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
viz_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

#doc.wasAssociatedWith(viz_brightkite, this_script)
#doc.wasAssociatedWith(viz_gowalla, this_script)
doc.wasAssociatedWith(viz_twitter, this_script)

#doc.used(viz_brightkite, brightkite_resource, startTime)
#doc.used(viz_gowalla, gowalla_resource, startTime)
doc.used(viz_twitter, twitter_resource, startTime)

#brightkite_ent = doc.entity('dat:brightkite', {prov.model.PROV_LABEL:'Brightkite data', prov.model.PROV_TYPE:'ont:DataSet'})
#doc.wasAttributedTo(brightkite_ent, this_script)
#doc.wasDerivedFrom(brightkite_ent, brightkite_resource)

#gowalla_ent = doc.entity('dat:gowalla', {prov.model.PROV_LABEL:'Gowalla dataset', prov.model.PROV_TYPE:'ont:DataSet'})
#doc.wasAttributedTo(gowalla_ent, this_script)
#doc.wasDerivedFrom(gowalla_ent, gowalla_resource)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter population dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


# 
