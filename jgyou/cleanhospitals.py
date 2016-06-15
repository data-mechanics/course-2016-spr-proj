'''
cleanhospitals.py

A script that has nothing to do with hospital hygiene
but has everything to do with updating field names,
incorporating fields, etc. to make data more amenable
to use later.

Parse apart address info and rename fields as needed
location_street_name
location
latitude
longitude


Rename
neighborhood
resource_name
neighborhood
location_zipcode
'''

from urllib import parse, request
from json import loads, dumps

import dml
import prov.model
import datetime
import uuid

def convertCase(s):
    strs = s.lower().split(" ")
    strs2 = [p[0].upper() + p[1:] for p in strs]
    return " ".join(strs2)

def make_provdoc(repo, runids, startTime, endTime):
    run_id = runids
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.


    this_script = provdoc.agent('alg:cleanhospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    resource = provdoc.entity('dat:hospitals', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = provdoc.activity('log:a'+run_id, startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
    output2 = provdoc.entity('dat:hospitalsjs', {prov.model.PROV_LABEL:'Hospitals', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})
    provdoc.wasAssociatedWith(this_run, this_script)
    provdoc.used(this_run, resource, startTime)

    output = resource
    provdoc.wasAttributedTo(output, this_script)
    provdoc.wasGeneratedBy(output, this_run, endTime)
    provdoc.wasDerivedFrom(output, resource, this_run, this_run, this_run)

    provdoc.wasAttributedTo(output2, this_script)
    provdoc.wasGeneratedBy(output2, this_run, endTime)
    provdoc.wasDerivedFrom(output2, resource, this_run, this_run, this_run)


    if startTime == None:
        plan = open('plan.json','r')
        docModel = prov.model.ProvDocument()
        doc = docModel.deserialize(plan)
        doc.update(provdoc)
        plan.close()
        plan = open('plan.json', 'w')
        plan.write(json.dumps(json.loads(doc.serialize()), indent=4))
        plan.close()
    else:
        repo.record(provdoc.serialize()) 


# set up connection

client = dml.pymongo.MongoClient()
repo = client.repo

with open("auth.json") as f:
    auth = json.loads(f.read())
    user = auth['user']

    repo.authenticate(user, user)

    startTime = datetime.datetime.now()


    #############

    # rename fields
    repo[user + '.hospitals'].update_many({},{"$rename": {'ad': 'location_street_name', 'name': 'resource_name', 'neigh': 'neighborhood', 'location_zip': 'location_zipcode'}})

    hospitalsjs = []

    # update other fields
    for h in repo[user + '.hospitals'].find():
        lon = h["location"]["coordinates"][0]
        lat = h["location"]["coordinates"][1]
        street = h["location_street_name"]
        streetupdated = convertCase(street)

        repo[user + '.hospitals'].update({"_id": h["_id"]}, {"$set": {"longitude": lon, "latitude": lat, "location_type": "hospital", "location_street_name": streetupdated}})

        addr = streetupdated + "," + " Boston, MA " + h["location_zipcode"]

        hospitalsjs.append({"name": h["resource_name"], "street": addr, "longitude": lon, "latitude": lat})

    with open("hospitalsgeo.json", "w") as hospitalout:
        hospitalout.write("hospitalsjs = " + json.dumps(hospitalsjs, indent=4) + ";")
        hospitalout.close()

        endTime = datetime.datetime.now()

        ###############

        run_id = str(uuid.uuid4())
        make_provdoc(repo, run_id, startTime, endTime)
        make_provdoc(repo, run_id, None, None)

        repo.logout()
