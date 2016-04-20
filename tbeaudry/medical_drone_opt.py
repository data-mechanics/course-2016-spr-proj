import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import time
from scipy.optimize import linprog

exec(open('../pymongo_dm.py').read())
startTime = datetime.datetime.now()
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('tbeaudry', 'tbeaudry')

repo.dropPermanent("HS_OPT")
repo.createPermanent("HS_OPT")


hos_stations=['Beth Israel Deaconess Medical Center East Cam',
              'Boston City Hospital',
              'Boston Specialty & Rehabilitation Hospital',
              'Boston Medical Center',
              "Brigham And Women's Hospital",
              'Carney Hospital',
              "Children's Hospital",
              'Dana-farber Cancer Institute',
              'Faulkner Hospital',
              "Franciscan Children's  Hospital",
              'Kindred Hospital',
              'Jewish Memorial Hospital',
              'Lemuel Shattuck Hospital',
              'Massachusetts Eye & Ear Infirmary',
              'Massachusetts General Hospital',
              'New England Baptist Hospital',
              'Beth Israel Deaconess Medical Center West Cam',
              'New England Medical Center',
              'Shriners Burns Institute',
              'Spaulding Rehabilitation Hospital',
              'Arbour Hospital',
              'Va Hospital',
              'VA Hospital',
              'Hebrew Rehabilitation Center']

dates={"2015-02-23T00:00:00":0,"2015-02-24T00:00:00":1,"2015-02-25T00:00:00":2,
       "2015-02-26T00:00:00":3,"2015-02-27T00:00:00":4,"2015-02-28T00:00:00":5,
       "2015-03-01T00:00:00":6,"2015-03-02T00:00:00":7,"2015-03-03T00:00:00":8}

#ins = repo.tbeaudry.HS_LOC.find()
events= repo.tbeaudry.HS_EVENTS.find()
radius=2
#how many drones total
total_d=30
#how many drones max and min in a single hospital
max_d=5
min_d=0
#how many missions a single drone can fly
max_fly=5
data={}
for y in range(0,len(hos_stations)):
    #here we can set an lower and upper bound on the number of drones we put in
    #each hospital. Potentially if drones were already set up to optimize placement
    #of additional drones
    data[hos_stations[y]]=([0,0,0,0,0,0,0,0,0],min_d,max_d)
ins=repo.tbeaudry.HS_EVENTS.find({})
for y in ins:
    d=dates[y['start_calendar_date']]
    for x in range(0,len(hos_stations)):
        if (y[hos_stations[x]]<radius):
            data[hos_stations[x]][0][d]+=1

#print(data)
bounds=[-1]*len(hos_stations)
for y in range(0,len(hos_stations)):
    bounds[y]=((data[hos_stations[y]][1],data[hos_stations[y]][2]))
print("the number of potentially satisfiable medical calls per hospital per day")
for x in range(0,len(dates)):
    c=[]
    d=x
    for y in range(0,len(hos_stations)):
        #here you can change the formula for calulating the number of missions a
        #hospital can serve in a single day
        c.append(-1*max_fly)
    #this is where you can add extra constraints, such as if you wanted
    #to limit the number of drones between multiple hospitals or if you wanted to
    #scale
    b = [-1]*(len(hos_stations)+1+4)
    for y in range(0,len(hos_stations)):
        #print(y,hos_stations[y],data[hos_stations[y]][0][d])
        b[y]=(data[hos_stations[y]][0][d])
    b[len(hos_stations)]=total_d
    b[len(hos_stations)+1]=5
    b[len(hos_stations)+2]=5
    b[len(hos_stations)+3]=5
    b[len(hos_stations)+4]=5
    print(b)
    A=[-1]*(len(hos_stations)+1+4)
    a=[]
    for y in range(0,len(hos_stations)):
        a.append(1)
    A[len(hos_stations)]=a
    for y in range(0,len(hos_stations)):
        xx=[0]*len(hos_stations)
        xx[y]=max_fly
        A[y]=xx
    for y in range(0,4):
        xx=[0]*len(hos_stations)
        if (y==0):
            xx[10]=1
            xx[9]  =1
        elif (y==1):
            xx[13]=1
            xx[14]=1
            xx[18]=1
            xx[19]=1

        elif (y==2):
            xx[0]=1
            xx[6]=1
            xx[7]=1
            xx[4]=1
            xx[16]=1
            xx[15]=1
        else:
            xx[1]=1
            xx[3]=1

        A[len(hos_stations)+1+y]=xx
        
        
    #print(A)
    #print(b)
        
    res = linprog(c, A_ub=A, b_ub=b, bounds=bounds,
                  options={"disp": True})
    print(res)
    #here i break because my implementation is flawed
    break
print("\nClearly this implementation using linear programming cant give good enough",
      " vaules of optimizaiton. I intend to try to use z3 in the future")    

endTime = datetime.datetime.now()
doc = prov.model.ProvDocument()

endTime = datetime.datetime.now()
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/tbeaudry/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/tbeaudry/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

comp_hs = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Computation':'Opt for med'})


this_script = doc.agent('alg:medical_drone_opt', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
HS_EVENTS  = doc.entity('dat:HS_EVENTS', {prov.model.PROV_LABEL:'Medical Events', prov.model.PROV_TYPE:'ont:DataSet'})
OPT_MED  = doc.entity('dat:OPT_MED', {prov.model.PROV_LABEL:'Medical Optimization', prov.model.PROV_TYPE:'ont:DataSet'})

doc.wasAttributedTo(OPT_MED, this_script)
doc.wasGeneratedBy(OPT_MED, comp_hs, endTime)
doc.wasDerivedFrom(OPT_MED,HS_EVENTS, comp_hs, comp_hs, comp_hs)





repo.record(doc.serialize()) # Record the provenance document.
print(json.dumps(json.loads(doc.serialize()), indent=4))
#print(doc.get_provn())
repo.logout()

    
