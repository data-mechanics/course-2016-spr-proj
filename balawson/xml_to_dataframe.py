# coding: utf-8
###############################################################
####   import dependancies       
###############################################################

import pandas as pd
import pymongo, datetime, uuid
import prov.model
import random
import xml.etree.ElementTree as xmltree
exec(open('../pymongo_dm.py').read())

###############################################################
####    parse the xml !!!!!       
###############################################################

startTime  = datetime.datetime.now()
roads, lat, lng, population, users = [], [] , [] , [] , [] #lists to store the information
e = xmltree.parse('BostonNetwork/outputs/roadnetwork.xml').getroot()
for intersection in e.findall('intersection'):
    children = intersection.getchildren() 
    r = ''
    for child in children:
        if child.get('name'):
            if len(r) < 1:
                r += ( child.get('name'))
            else:
                r += ', ' + ( child.get('name'))
        elif child.get('num') :
            pop = child.get('num')
        elif child.get('ulist') :
            user = child.get('ulist')
        else:
            pass
    users += [user]
    lat += [intersection.get('lat')]
    lng += [intersection.get('long')]
    roads.append(r)
    population.append(pop)

###############################################################
####    make dataframe
###############################################################

d = {'roads'    : pd.Series(roads) ,
    'lat'       : pd.Series(lat)      ,
    'lng'       : pd.Series(lng)      ,
    'population': pd.Series(population) , 
    'users': pd.Series(users) , 
    }

df = pd.DataFrame(d)
#df.to_csv('intersectionspopulation.csv')

###############################################################
####    dump dataframe
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

collection_name = 'twitter-intersection'
records = json.loads(df.T.to_json()).values()
repo.dropPermanent(collection_name)
repo.createPermanent(collection_name)
repo['balawson.' + collection_name].insert_many(records)

endTime = datetime.datetime.now()
###############################################################
####    record provanence       
###############################################################
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/balawson/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/balawson/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
#doc.add_namespace('snap', 'https://snap.stanford.edu/data/')
doc.add_namespace('bal', 'http://people.bu.edu/balawson/')

this_script = doc.agent('alg:compare', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
#brightkite_resource = doc.entity('snap:brightkite', {'prov:label':'SNAP: Standford Network Analysis Project - Brightkite', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
#gowalla_resource = doc.entity('snap:gowalla', {'prov:label':'SNAP: Standford Network Analysis Project - Gowalla', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})
twitter_xml = doc.entity('bal:twitter', {'prov:label':'Derived intermediate state of curated Tweets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'xml'})

#viz_brightkite = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

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
