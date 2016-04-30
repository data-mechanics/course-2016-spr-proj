#coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import prov.model
import pymongo, datetime, uuid, math, os, subprocess
import mongo_to_xml, xml_to_dataframe
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
#tweets     = pd.DataFrame(list(repo.balawson.twitter.find()))
bigtweets     = pd.DataFrame(list(repo.balawson.big_twitter.find()))
#gowalla    = pd.DataFrame(list(repo.balawson.gowalla.find()))
#brightkite = pd.DataFrame(list(repo.balawson.brightkite.find()))

#tweets.time = tweets.time.apply(lambda d: pd.to_datetime(d*1000000))
bigtweets.time = bigtweets.time.apply(lambda d: pd.to_datetime(d*1000000))

###############################################################
####    group the data by time of day
###############################################################

#groups = bigtweets.groupby(bigtweets.time.map(lambda t: str(t.hour)))
groups = bigtweets.groupby(pd.Grouper(key='time', freq='M'))

source = 'bigtwitter'
for key, group in groups:
    mongo_to_xml.to_xml(group, 'temp.xml') #save input 
 
    with cd("./BostonNetwork/src/"):
        #print (os.getcwd())
        os.popen("javac getintersections/RoadNetwork.java;")
        os.wait()
        pipe = os.popen("java  getintersections.RoadNetwork")
        print([line for line in pipe.readlines()], end='')
        print()
        os.wait()

    temp_df = xml_to_dataframe.xml_to_dataframe('./BostonNetwork/outputs/roadnetwork.xml')
    xml_to_dataframe.insert_df_to_mongo(temp_df, str(source + "date" + str(key)))
    print('.', end='')

###############################################################
####    save results       
###############################################################

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

this_script = doc.agent('alg:compare', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
twitter_xml = doc.entity('bal:twitter', {'prov:label':'Derived intermediate state of curated Tweets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'xml'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

doc.wasAssociatedWith(twitter_xml, this_script)
doc.used(twitter_xml, twitter_resource, startTime)   

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
