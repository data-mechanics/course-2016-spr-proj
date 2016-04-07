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

###############################################################
####   declare function
###############################################################

def to_xml(df, filename=None, mode='w'):
    #print df.columns
    res = '''<?xml version="1.0"?>
<ROWSET>
<ROW FIELD1="" FIELD2="user_id" FIELD3="lng" FIELD4="lat" FIELD5="time" ></ROW> 
    '''
    def row_to_xml(row, idx):
        xml = ['<ROW']
        xml += ['FIELD1="{0}"'.format(idx)]
        try:
            xml += ['FIELD2="{0}"'.format(row.user_id)] 
        except:  
            xml += ['FIELD2="{0}"'.format(row.user)]
        xml += ['FIELD3="{0}"'.format(row.lng)]
        xml += ['FIELD4="{0}"'.format(row.lat)]
        xml += ['FIELD5="{0}"'.format(str(row.time).strip().replace('\n', ' '))]
        xml.append('></ROW>\n')
        return ' '.join(xml)
        

    for idx, row in df.iterrows():
        res += row_to_xml(row, idx)
    res += '\n </ROWSET>'

    if filename is None:
        return res

    with open(filename, mode) as f:
        f.write(res)

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

###############################################################
####    save results to temp csv
###############################################################

to_xml(tweets, 'twitter.xml')
to_xml(gowalla, 'gowalla.xml')
to_xml(brightkite, 'brightkite.xml')

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
