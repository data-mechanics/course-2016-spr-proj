import requests
import json
import pymongo
import prov.model
import datetime
import uuid, os, time
import pandas as pd

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
username = 'balawson'
pwd      = 'balawson'
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(username, pwd)

# Retrieve some data sets (not using the API here for the sake of simplicity).


startTime = datetime.datetime.now()
def download_fiter_and_insert(filename, url, collection_name, header = ['user', 'time', 'lat', 'lng', 'location']):
    print('Begin downloading {0}'.format(filename))
    response = requests.get(url, stream=True) 
    with open(filename, "wb") as handle:
       for data in response.iter_content(chunk_size=1024):
           handle.write(data)
    print('Finish downloading {0}'.format(filename))
    ###unzip###
    os.popen('gzip -d {0}'.format(filename))
    ####read###
    time.sleep(30) #need to wait until done unzipping
    print('Filtering {0}'.format(filename))
    df = pd.read_csv('{0}'.format(filename.split('.gz')[0]),delimiter = '\t', names = header)
    
    df =  df[ (df.lat < 42.445945) & (df.lat > 42.275086) & (df.lng > -71.194213) & (df.lng < -70.926301)]
    print('Inserting {0} into mongo'.format(filename))
    records = json.loads(df.T.to_json()).values()
    repo.dropPermanent(collection_name)
    repo.createPermanent(collection_name)
    repo['balawson.' + collection_name].insert_many(records)

brightkite_filename = 'brightkite.txt.gz'
brightkite_url      = 'https://snap.stanford.edu/data/loc-brightkite_totalCheckins.txt.gz'
download_fiter_and_insert(brightkite_filename, brightkite_url, 'brightkite', header = ['user', 'time', 'lat', 'lng', 'location'])

gowalla_filename = 'gowalla.txt.gz'
gowalla_url      = 'https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz'
download_fiter_and_insert(gowalla_filename, gowalla_url, 'gowalla', header = ['user', 'time', 'lat', 'lng', 'location'])

twitter_filename = '2016-02-25.sample.csv'
twitter_url      = 'http://people.bu.edu/balawson/2016-02-25.sample.csv' 

def download_and_insert(filename, url, collection_name, header = ['lat', 'location', 'lng', 'source', 'time', '_id', 'user']):
    print('Begin downloading {0}'.format(filename))
    response = requests.get(url, stream=True) 
    with open(filename, "wb") as handle:
       for data in response.iter_content(chunk_size=1024):
           handle.write(data)
    print('Finish downloading {0}'.format(filename))
    ####read###
    df = pd.read_csv('{0}'.format(filename), header = 0, names = header)
    df.drop('source', inplace=True, axis=1)
    print(df.head())
    df =  df[ (df.lat < 42.445945) & (df.lat > 42.275086) & (df.lng > -71.194213) & (df.lng < -70.926301)]
    print('Inserting {0} into mongo'.format(filename))
    records = json.loads(df.T.to_json()).values()
    repo.dropPermanent(collection_name)
    repo.createPermanent(collection_name)
    repo['balawson.' + collection_name].insert_many(records)
download_and_insert(twitter_filename, twitter_url, 'twitter')
endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/balawson/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/balawson/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('snap', 'https://snap.stanford.edu/data/')
doc.add_namespace('bal', 'http://people.bu.edu/balawson/')

this_script = doc.agent('alg:collect', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
brightkite_resource = doc.entity('snap:brightkite', {'prov:label':'SNAP: Standford Network Analysis Project - Brightkite', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})

gowalla_resource = doc.entity('snap:gowalla', {'prov:label':'SNAP: Standford Network Analysis Project - Gowalla', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txtgz'})

twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

get_brightkite = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
get_gowalla = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
get_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

doc.wasAssociatedWith(get_brightkite, this_script)
doc.wasAssociatedWith(get_gowalla, this_script)
doc.wasAssociatedWith(get_twitter, this_script)

doc.used(get_brightkite, brightkite_resource, startTime)
doc.used(get_gowalla, gowalla_resource, startTime)
doc.used(get_twitter, twitter_resource, startTime)

brightkite_ent = doc.entity('dat:brightkite', {prov.model.PROV_LABEL:'Brightkite data', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(brightkite_ent, this_script)
doc.wasGeneratedBy(brightkite_ent, get_brightkite, endTime)
doc.wasDerivedFrom(brightkite_ent, brightkite_resource, get_brightkite, get_brightkite, get_brightkite)

gowalla_ent = doc.entity('dat:gowalla', {prov.model.PROV_LABEL:'Gowalla dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(gowalla_ent, this_script)
doc.wasGeneratedBy(gowalla_ent, get_gowalla, endTime)
doc.wasDerivedFrom(gowalla_ent, gowalla_resource, get_gowalla, get_gowalla, get_gowalla)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasGeneratedBy(twitter_ent, get_twitter, endTime)
doc.wasDerivedFrom(twitter_ent, twitter_resource, get_twitter, get_twitter, get_twitter)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()

## eof
