import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import time
import geopy
from geopy import Nominatim
from geopy.distance import vincenty

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('tbeaudry', 'tbeaudry')

startTime = datetime.datetime.now()

pol_events= ['TRAFFIC PURSUIT','PROTESTERS GATHERING','PERSON SCREAMING FOR HELP','PERSON WITH A KNIFE','SHOTS FIRED', 'PERSON WITH A GUN', 'ROBBERY','Traffic Enforcement', 'TRAFFIC STOP','Tagging','VANDALISM','ALARM', 'DISTURBANCE', 'ASSAULT IN PROGRESS','KIDNAPPING','HOME INVASION','BREAKING AND ENTERING']
med_events= ['Request EMS and BPD Response', 'UNABLE TO DETERMINE IF CONS/MOVING   (E) (F) (P)', 'UNCONSCIOUS PERSON   (E) (F) (P)', 'CARDIAC EVENT','ASSIST EMS OFFICIALS ONSCENE   (E) (P)','BURNS','PERSON STABBED   (P) (E)','MOTOR VEHICLE ACCIDENT','INJURY']

pol_stations=['District A-1 Police Station', 'District D-4 Police', 'District E-13 Police Station', 'District B-3 Police Station', 'District E-18 Police Station', 'District D-14 Police Station', 'Boston Police Headquarters', 'District A-7 Police Station', 'District C-6 Police Station', 'District B-2 Police Station', 'District E-5 Police Station', 'District C-11 Police Station']
hos_stations=['Beth Israel Deaconess Medical Center East Cam', 'Boston City Hospital', 'Boston Specialty & Rehabilitation Hospital', 'Boston Medical Center', "Brigham And Women's Hospital", 'Carney Hospital', "Children's Hospital", 'Dana-farber Cancer Institute', 'Faulkner Hospital', "Franciscan Children's  Hospital", 'Kindred Hospital', 'Jewish Memorial Hospital', 'Lemuel Shattuck Hospital', 'Massachusetts Eye & Ear Infirmary', 'Massachusetts General Hospital', 'New England Baptist Hospital', 'Beth Israel Deaconess Medical Center West Cam', 'New England Medical Center', "St. Elizabeth's Hospital", "St. Margaret's Hospital For Women", 'Shriners Burns Institute', 'Spaulding Rehabilitation Hospital', 'Arbour Hospital', 'Va Hospital', 'VA Hospital', 'Hebrew Rehabilitation Center']


repo.dropPermanent("PS_EVENTS")
repo.dropPermanent("HS_EVENTS")
repo.createPermanent("PS_EVENTS")
repo.createPermanent("HS_EVENTS")

print('CREATING PS_EVENTS')
geolocator = Nominatim()
for x in pol_events:
    ins=repo.tbeaudry.P_911.find({'cad_event_type':x})
    for y in ins:
        lat=0
        lon =0
        if 'longitude' in y:
            lat=y['latitude']
            lon=y['longitude']
        elif 'udo_event_location_full_street_address' in y:
            '''

            continue here to run quick version

            '''
            loc=y['udo_event_location_full_street_address']+" Boston"
            loc2=geolocator.geocode(loc)
            if (loc2==None):
                continue
            lat = loc2.latitude
            lon = loc2.longitude
        for z in pol_stations:
            if (lat!=0 and lon!=0):
                pol=repo.tbeaudry.PS_LOC.find_one({'name':z})
                y[z]=geopy.distance.vincenty((lat,lon), (pol['location']['latitude'],pol['location']['longitude'])).miles
            else:
                y[z]=999
        if lat!=0 and lon!=0:
            
            repo.tbeaudry.PS_EVENTS.insert_one(y)

            
print("PS EVENTS DONE\n")
print('CREATING HS_EVENTS')

for x in med_events:
    ins=repo.tbeaudry.P_911.find({'cad_event_type':x})
    for y in ins:
        lat=0
        lon =0
        if 'longitude' in y:
            lat=y['latitude']
            lon=y['longitude']
        elif 'udo_event_location_full_street_address' in y:
            '''

            continue here to run quick version

            '''
            loc=y['udo_event_location_full_street_address']+" Boston"
            loc2=geolocator.geocode(loc)
            if (loc2==None):
                continue
            lat = loc2.latitude
            lon = loc2.longitude
        for z in hos_stations:
            
            if (lat!=0 and lon!=0):
                hos=repo.tbeaudry.HS_LOC.find_one({'name':z})
                if z=="St. Elizabeth's Hospital" :
                    z="St Elizabeth's Hospital"
                if z =="St. Margaret's Hospital For Women":
                    z="St Margaret's Hospital For Women"

                
                y[z]=geopy.distance.vincenty((lat,lon), (hos['location']['latitude'],hos['location']['longitude'])).miles
            else:
                if z=="St. Elizabeth's Hospital" :
                    z="St Elizabeth's Hospital"
                if z =="St. Margaret's Hospital For Women":
                    z="St Margaret's Hospital For Women"
                y[z]=999

        if lat!=0 and lon!=0:
            
            repo.tbeaudry.HS_EVENTS.insert_one(y)

        
print ("HS EVENTS DONE\n")

print('Cumulating for ps')
for x in pol_stations:
    new=repo.tbeaudry.PS_LOC.find_one({'name':x})
    new['Top3']=0
    repo.tbeaudry.PS_LOC.remove({'name':x})
    repo.tbeaudry.PS_LOC.insert_one(new)
    '''
    print(repo.tbeaudry.PS_LOC.find_one({'name':x}))
    '''
    
    ins=repo.tbeaudry.PS_EVENTS.find({})
    for y in ins:
        min1=999
        min2=999
        min3=999
        for z in pol_stations:
            if y[z]<min1:
                min1=y[z]
            elif y[z]<min2:
                min2=y[z]
            elif y[z]<min3:
                min3=y[z]

        if y[x]<=min3:
            val=repo.tbeaudry.PS_LOC.find_one({'name':x})
            val=val['Top3']
            new=repo.tbeaudry.PS_LOC.find_one({'name':x})
            new['Top3']=val+1
            repo.tbeaudry.PS_LOC.remove({'name':x})
            repo.tbeaudry.PS_LOC.insert_one(new)
        '''
        print(repo.tbeaudry.PS_LOC.find_one({'name':x})['name'],repo.tbeaudry.PS_LOC.find_one({'name':x})['Top3'])
    '''
        
print("Cumulating for hs")
for x in hos_stations:
    new=repo.tbeaudry.HS_LOC.find_one({'name':x})
    new['Top3']=0
    repo.tbeaudry.HS_LOC.remove({'name':x})
    repo.tbeaudry.HS_LOC.insert_one(new)
    '''
    print(repo.tbeaudry.PS_LOC.find_one({'name':x}))
    '''
    
    ins=repo.tbeaudry.HS_EVENTS.find({})
    for y in ins:
        min1=999
        min2=999
        min3=999
        for z in hos_stations:
            if z=="St. Elizabeth's Hospital" :
                z="St Elizabeth's Hospital"
            if z =="St. Margaret's Hospital For Women":
                z="St Margaret's Hospital For Women"
            if y[z]<min1:
                min1=y[z]
            elif y[z]<min2:
                min2=y[z]
            elif y[z]<min3:
                min3=y[z]
        if x=="St. Elizabeth's Hospital" :
            x="St Elizabeth's Hospital"
        if x =="St. Margaret's Hospital For Women":
            x="St Margaret's Hospital For Women"
        if y[x]<=min3:
            if x=="St Elizabeth's Hospital" :
                x="St. Elizabeth's Hospital"
            if x =="St Margaret's Hospital For Women":
                x="St. Margaret's Hospital For Women"


            
            val=repo.tbeaudry.HS_LOC.find_one({'name':x})
            val=val['Top3']
            new=repo.tbeaudry.HS_LOC.find_one({'name':x})
            new['Top3']=val+1
            repo.tbeaudry.HS_LOC.remove({'name':x})
            repo.tbeaudry.HS_LOC.insert_one(new)
print("All Done")

endTime = datetime.datetime.now()
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/tbeaudry/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/tbeaudry/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:reform', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

PS_LOC = doc.entity('dat:PS_LOC', {prov.model.PROV_LABEL:'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
HS_LOC = doc.entity('dat:HS_LOC', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
P_911  = doc.entity('dat:P_911', {prov.model.PROV_LABEL:'Boston Police Department 911', prov.model.PROV_TYPE:'ont:DataSet'})

comp_ps = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Computation':'Distance to PS'})
comp_hs = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Computation':'Distance to HS'})

doc.used(PS_LOC, comp_ps, startTime)
doc.used(HS_LOC , comp_hs, startTime)

doc.wasAssociatedWith(PS_LOC, this_script)
doc.wasAssociatedWith(HS_LOC, this_script)
doc.wasAssociatedWith(P_911, this_script)


PS_EVENTS  = doc.entity('dat:PS_EVENTS', {prov.model.PROV_LABEL:'Police Events', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(PS_EVENTS, this_script)
doc.wasGeneratedBy(PS_EVENTS, comp_ps, endTime)
doc.wasDerivedFrom(PS_EVENTS, PS_LOC, comp_ps, comp_ps, comp_ps)

HS_EVENTS  = doc.entity('dat:HS_EVENTS', {prov.model.PROV_LABEL:'Medical Events', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(HS_EVENTS, this_script)
doc.wasGeneratedBy(HS_EVENTS, comp_hs, endTime)
doc.wasDerivedFrom(HS_EVENTS, HS_LOC, comp_hs, comp_hs, comp_hs)


repo.record(doc.serialize()) # Record the provenance document.
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
print(doc.get_provn())
plan.close()

#print(doc.get_provn())
repo.logout()







    
    
