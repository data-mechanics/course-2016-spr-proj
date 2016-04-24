# coding: utf-8
###############################################################
####   import dependancies       
###############################################################
import pandas as pd
import pymongo, datetime, uuid, math
import prov.model
import shapefile
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
            total += int(row[col])
        except:
            pass
    return total

def color_map(df):
    my_colors = {'Allston/Brighton': [0.8823529411764706, 0.5843137254901961,  0.5215686274509804],
 'Back Bay/Beacon Hill': [0.7333333333333333,
  0.5176470588235295,
  0.4666666666666667],
 'Central': [0.7529411764705882, 0.7372549019607844, 0.8392156862745098],
 'Charlestown': [0.7254901960784313, 0.49019607843137253, 0.5294117647058824],
 'East Boston': [0.00784313725490196, 0.6470588235294118, 0.24705882352941178],
 'Fenway/Kenmore': [0.2901960784313726,
  0.8901960784313725,
  0.43529411764705883],
 'Harbor Islands': [0.9411764705882353,
  0.7254901960784313,
  0.5529411764705883],
 'Hyde Park': [0.9176470588235294, 0.8274509803921568, 0.7764705882352941],
 'Jamaica Plain': [0.8901960784313725, 0.7333333333333333, 0.7098039215686275],
 'Mattapan': [0.2196078431372549, 0.06666666666666667, 0.7764705882352941],
 'North Dorchester': [0.8784313725490196,
  0.5686274509803921,
  0.4823529411764706],
 'Roslindale': [0.5764705882352941, 0.8352941176470589, 0.5529411764705883],
 'Roxbury': [0.7254901960784313, 0.9019607843137255, 0.6862745098039216],
 'South Boston': [0.7568627450980392, 0.8313725490196079, 0.7450980392156863],
 'South Dorchester': [0.41568627450980394,
  0.8274509803921568,
  0.24705882352941178],
 'South End': [0.23137254901960785, 0.023529411764705882, 0.5568627450980392],
 'West Roxbury': [0.7764705882352941, 0.8705882352941177, 0.7803921568627451]}

    color = []
    for idx, x in df.iterrows():
        try:
            color.append(my_colors[x.neighborhoods])
        except KeyError:
            color.append('k')
    return my_colors, color

def make_patch(color, label):
    return mpatches.Patch(color=color, label=label)

tweets = tweets.convert_objects(convert_numeric=True) #TODO this is getting deprenciated
tweets['total'] = tweets.apply(lambda d: countpeople( d), axis=1)
tweets.drop(['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19','2', '20', '21', '22', '23', '3', '4', '5', '6', '7', '8', '9', '_id',], inplace=True, axis=1)
tweets['lat'] = tweets.lat.apply(lambda d: float(d))
tweets['lng'] = tweets.lng.apply(lambda d: float(d))
tweets['neighborhoods'] = tweets.apply(lambda d: determine_neighborhood( d.lat, d.lng), axis=1) 


###############################################################
####    make some summary charts
###############################################################

sorted_tweets = tweets.sort_values(by='total', ascending=False)
cmap, colorful = color_map(sorted_tweets)

leg = []
for key in cmap:
    leg.append(make_patch(label=key, color=cmap[key]))

plt.gcf().subplots_adjust(bottom=0.22)
plt.xlabel('Neighborhood')
plt.ylabel('Number of Intersections')
plt.title('Intersections by Neighborhood')
tweets.neighborhoods.value_counts().plot(kind='bar')
plt.savefig('img/numberofintersections.png')

sorted_tweets[:25].total.plot(kind='bar', color=colorful, figsize=(10,10))
#plt.legend(handles=leg, loc='center left', bbox_to_anchor=(1, 0.5))
plt.legend(handles=leg)
plt.title("Top 25 most popular intersections")
plt.savefig('img/popularintersections.png')


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
doc.add_namespace('bos', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

this_script = doc.agent('alg:checkzone', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
twitter_resource = doc.entity('bal:twitter', {'prov:label':'Sample of Curated Tweet', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
shapefile_resource = doc.entity('bos:shapefile', {'prov:label':'BostonMaps: Open Data | Planning Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'zip'})

comp_twitter = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(comp_twitter, this_script)

doc.used(comp_twitter, twitter_resource, startTime)
doc.used(comp_twitter, shapefile_resource, startTime)

twitter_ent = doc.entity('dat:twitter', {prov.model.PROV_LABEL:'neighboorhoods for intersections', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(twitter_ent, this_script)
doc.wasDerivedFrom(twitter_ent, twitter_resource)

shapefile_ent = doc.entity('bos:shapefile', {prov.model.PROV_LABEL:'BostonMaps: Open Data | Planning Districts', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(shapefile_ent, this_script)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


# 
