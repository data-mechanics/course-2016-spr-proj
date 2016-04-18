# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, math
import prov.model
import shapefile
exec(open('../pymongo_dm.py').read())
###############################################################
####    access the data       
###############################################################
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('balawson', 'balawson')

startTime  = datetime.datetime.now()
tweets     = pd.DataFrame(list(repo.balawson.twitterIntersectionHours.find()))


sf = shapefile.Reader("./Planning_Districts/Planning_Districts.shp")

def determine_neighborhood(lat, lng):
    for item in sf.shapeRecords():
        lng1, lat1,lng2, lat2 = item.shape.bbox
        if (lng1 < lng < lng2) and (lat1 < lat < lat2):
            return item.record[3]
    return None
        

#gowalla    = pd.DataFrame(list(repo.balawson.gowalla.find()))
#brightkite = pd.DataFrame(list(repo.balawson.brightkite.find()))

###############################################################
####    using shape files determine which neighboorhood intersection is in
###############################################################
def countpeople(row):
    total = 0;
    for col in row.keys():
        try:
            int(col)
            total += row[col] 
        except:
            pass
    return total


tweets.drop(['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19','2', '20', '21', '22', '23', '3', '4', '5', '6', '7', '8', '9', '_id',], inplace=True, axis=1)
tweets['lat'] = tweets.lat.apply(lambda d: float(d))
tweets['lng'] = tweets.lng.apply(lambda d: float(d))
tweets.neighborhoods = tweets.apply(lambda d: determine_neighborhood( d.lat, d.lng), axis=1) 


###############################################################
####    make some summary charts
###############################################################

plt.gcf().subplots_adjust(bottom=0.22)
plt.xlabel('Neighborhood')
plt.ylabel('Number of Intersections')
plt.title('Intersections by Neighborhood')
tweets.neighborhood.value_counts().plot(kind='bar')
plt.savefig('img/numberofintersections.png')

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
