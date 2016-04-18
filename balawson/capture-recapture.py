###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, prov
exec(open('../pymongo_dm.py').read())

###############################################################
####    access the data       
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()
tweets     = pd.DataFrame(list(repo.balawson.twitter.find()))
tweets.time = tweets.time.apply(lambda d: pd.to_datetime(d*1000000))


###############################################################
####    compute!
###############################################################

first_date  = '2015-05-18'
second_date = '2015-05-22'

groups = tweets.groupby(tweets.time.map(lambda t: str(t.date())))

first_visit  = list(set(groups.get_group(first_date).user))
second_visit = list(set(groups.get_group(second_date).user))

N = 0 #number of animals in the population
n = len(first_visit)  #number of animals marked on first visit
K = len(second_visit) #number of animals captured on the second visit
k = sum([1 if x in first_visit else 0 for x in second_visit])

N = (K*n) / float(k)

###############################################################
####  dump to db
###############################################################

records = {'first_date'  : first_date  ,\
           'second_date' : second_date ,\
           'N'           : N           ,\
           'date_created': datetime.datetime.today(),
           'source'           : 'twitter'
          }
  
collection_name = 'capture_recapture'
repo.dropPermanent(collection_name)
repo.createPermanent(collection_name)

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

this_script = doc.agent('alg:capture-recapture', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

twitter_activity = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(twitter_activity, this_script)

doc.used(viz_twitter, twitter_resource, startTime)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'Twitter dataset', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


# 

