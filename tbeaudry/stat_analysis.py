import urllib.request
import json
import dml
import prov.model
import time
from datetime import datetime
import uuid
import time
from random import shuffle
from math import sqrt
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('tbeaudry', 'tbeaudry')

startTime = datetime.now()
def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)    






events= repo.tbeaudry.HS_EVENTS.find()

data=[]
datax=[]
datay=[]
data2=[]
data2x=[]
data2y=[]
for x in events:
    lat=x['latitude']
    long=x['longitude']
    time=datetime.strptime(x['start_standard_time'], '%I:%M:%S %p')
    t=datetime.strptime("6:00:00 AM", '%I:%M:%S %p')
    t2=datetime.strptime("12:00:00 AM 1900-01-01", '%I:%M:%S %p %Y-%m-%d')
    
    sec=((time-t).total_seconds())/60
    if (sec<0):
        sec=18*60+(time-t2).total_seconds()/60
        x=1
    


    
    data.append((float(lat),float(sec)))
    datax.append((float(lat)))
    datay.append(float(sec))
    data2.append((float(long),float(sec)))
    data2x.append((float(long)))
    data2y.append(float(sec))

x = [xi for (xi, yi) in data]
y = [yi for (xi, yi) in data]



print("Med Events")
print("For latitude vs minutes past 6 AM")
print("Original correlation and covari",corr(x,y),cov(x,y))
print("Cov and p value",scipy.stats.pearsonr(x, y))
plt.scatter(datax,datay)
plt.show()

x = [xi for (xi, yi) in data2]
y = [yi for (xi, yi) in data2]
xx = [xi for (xi, yi) in data2]
yy = [yi for (xi, yi) in data2]

print("For longitude vs minutes past 6 AM")
print("Original correlation and covari",corr(x,y),cov(x,y))
print("Cov and p value",scipy.stats.pearsonr(x, y))

plt.scatter(data2x,data2y)
plt.show()



repo.dropPermanent("HS_EVENTS_STATS")
repo.createPermanent("HS_EVENTS_STATS")
x = [xi for (xi, yi) in data]
y = [yi for (xi, yi) in data]


y={'datax':data,'datay':data2,'xcor,p':scipy.stats.pearsonr(x, y),'ycor,p':scipy.stats.pearsonr(xx, yy)}
repo.tbeaudry.HS_EVENTS_STATS.insert_one(y)

print("Police Events")

events= repo.tbeaudry.PS_EVENTS.find()

data=[]
datax=[]
datay=[]
data2=[]
data2x=[]
data2y=[]
for x in events:
    lat=x['latitude']
    long=x['longitude']
    if (float(lat)<42 or float(lat)>42.8 or float(long)<-74):
        continue
    time=datetime.strptime(x['start_standard_time'], '%I:%M:%S %p')
    t=datetime.strptime("6:00:00 AM", '%I:%M:%S %p')
    t2=datetime.strptime("12:00:00 AM 1900-01-01", '%I:%M:%S %p %Y-%m-%d')
    
    sec=((time-t).total_seconds())/60
    if (sec<0):
        sec=18*60+(time-t2).total_seconds()/60
        x=1
    


    
    data.append((float(lat),float(sec)))
    datax.append((float(lat)))
    datay.append(float(sec))
    data2.append((float(long),float(sec)))
    data2x.append((float(long)))
    data2y.append(float(sec))

x = [xi for (xi, yi) in data]
y = [yi for (xi, yi) in data]



print("For latitude vs minutes past 6 AM")
print("Original correlation and covari",corr(x,y),cov(x,y))
print("Cov and p value",scipy.stats.pearsonr(x, y))
plt.scatter(datax,datay)
plt.show()

x = [xi for (xi, yi) in data2]
y = [yi for (xi, yi) in data2]
xx = [xi for (xi, yi) in data2]
yy = [yi for (xi, yi) in data2]

print("For longitude vs minutes past 6 AM")
print("Original correlation and covari",corr(x,y),cov(x,y))
print("Cov and p value",scipy.stats.pearsonr(x, y))

plt.scatter(data2x,data2y)
plt.show()



repo.dropPermanent("PS_EVENTS_STATS")
repo.createPermanent("PS_EVENTS_STATS")

x = [xi for (xi, yi) in data]
y = [yi for (xi, yi) in data]

y={'datax':data,'datay':data2,'xcor,p':scipy.stats.pearsonr(x, y),'ycor,p':scipy.stats.pearsonr(xx, yy)}
repo.tbeaudry.PS_EVENTS_STATS.insert_one(y)

endTime = datetime.now()
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/tbeaudry/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/tbeaudry/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:stat_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

comp_ps = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Computation':'Seconds after 6 PS'})
comp_hs = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Computation':'Seconds after 6 HS'})

PS_EVENTS  = doc.entity('dat:PS_EVENTS', {prov.model.PROV_LABEL:'Police Events', prov.model.PROV_TYPE:'ont:DataSet'})
HS_EVENTS  = doc.entity('dat:HS_EVENTS', {prov.model.PROV_LABEL:'Medical Events', prov.model.PROV_TYPE:'ont:DataSet'})


PS_EVENTS_STATS  = doc.entity('dat:PS_EVENTS_STATS', {prov.model.PROV_LABEL:'Police Events Stats', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(PS_EVENTS_STATS, this_script)
doc.wasGeneratedBy(PS_EVENTS_STATS, comp_ps, endTime)
doc.wasDerivedFrom(PS_EVENTS_STATS, PS_EVENTS,comp_ps, comp_ps, comp_ps)

HS_EVENTS_STATS  = doc.entity('dat:HS_EVENTS_STATS', {prov.model.PROV_LABEL:'Medical Events Stats', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(HS_EVENTS_STATS, this_script)
doc.wasGeneratedBy(HS_EVENTS_STATS, comp_hs, endTime)
doc.wasDerivedFrom(HS_EVENTS_STATS, HS_EVENTS, comp_hs, comp_hs, comp_hs)

doc.used(PS_EVENTS_STATS, comp_ps, startTime)
doc.used(HS_EVENTS_STATS , comp_hs, startTime)

doc.wasAssociatedWith(PS_EVENTS_STATS, this_script)
doc.wasAssociatedWith(HS_EVENTS_STATS, this_script)

repo.record(doc.serialize()) # Record the provenance document.
print(json.dumps(json.loads(doc.serialize()), indent=4))
repo.logout()






