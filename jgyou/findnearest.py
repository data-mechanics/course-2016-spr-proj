'''
series of helper functions that find the nearest desired service (e.g. hospital)
given a series of coordinates for a service (e.g. coordinates for all hospitals) 
and a starting location; or that find
the group of coordinates that fall within a certain radius of the starting location (e.g. MBTA)
'''

import pymongo
import prov.model
import datetime
import uuid
import time

from urllib import request, parse
import json
from geopy.distance import vincenty


exec(open('../pymongo_dm.py').read())

# calls mapquest to get walking distance for two lat-long coordinates
def findDistance(repo, user, startLocation, endLocation):
    (startlat, startlon) = startLocation
    (endlat, endlon) = endLocation

    with open("auth.json") as authfile:
        auth = json.loads(authfile.read())
        key = auth["service"]["mapquest"]["key"]
    

        # query = "http://www.mapquestapi.com/directions/v2/route?key=" + \
        #     key + "&from=" + str(startlat) + "," + str(startlon) + "&to=" + str(endlat) + "," + str(endlon) \
        #     + "&outFormat=json" + "&routeType=pedestrian"

        query = "http://www.mapquestapi.com/directions/v2/route?key=" + \
             key + "&from=" + str(startlat) + "," + str(startlon) + "&to=" + str(endlat) + "," + str(endlon) \
             + "&outFormat=json" + "&routeType=pedestrian" + "&doReverseGeocode=false"

        response = request.urlopen(query).read().decode("utf-8")
        dist = json.loads(response)["route"]["distance"]

        return (dist, query)


# given a series of locations in a collection and single start location
# find the nearest location via walking distance
def findClosestCoordinate(repo, user, collec, collecName, startLocation):

    startTime = datetime.datetime.now()

    cursor = repo[collec].find()
    alldist = []
    queries = []

    for document in cursor:
        endLocation = (document["latitude"], document["longitude"])

        (dist, query) = findDistance(repo, user, startLocation, endLocation)

        queries.append(query)

        alldist.append([endLocation, dist])

    alldist2 = [b for [a, b] in alldist]
    mindist = min(alldist2)

    endTime = datetime.datetime.now()
    runids = [str(uuid.uuid4()) for i in range(len(queries))]

    makeProvFindCoordinates(repo, user, runids, startTime, endTime, queries, collec, collecName)
    makeProvFindCoordinates(repo, user, runids, None, None, queries, collec, collecName)

    return mindist

# for a given collection of sites and a start location,
# find if distance between site and start is less than
# some upper bound
def boundedRadiusMBTA(repo, user, collec, startLocation, bound):

    startTime = datetime.datetime.now()

    cursor = repo[collec].find()
    object_ids = []
    for document in cursor:
        (endlat, endlon) = (document["latitude"], document["longitude"])
        endLocation = (endlat, endlon)
        dist = vincenty(startLocation, endLocation).miles
        if dist <= bound:
            object_ids.append((document["_id"], dist, endlat, endlon, document["stop_name"], document["wheelchair"]))

    endTime = datetime.datetime.now()

    run_id = str(uuid.uuid4())

    makeProvMBTA(repo, user, run_id, startTime, endTime)
    makeProvMBTA(repo, user, run_id, None, None)

    return object_ids


def makeProvFindCoordinates(repo, user, run_ids, startTime, endTime, queries, collec, collecName):
    repo.authenticate(user, user)

    collec = collec.replace(user + ".", "")

    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    provdoc.add_namespace('ocd', 'https://api.opencagedata.com/geocode/v1/')

    # activity = invocation of script, agent = script, entity = resource
    # agent
    this_script = provdoc.agent('alg:findnearest', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # input data
    opencage = provdoc.entity('ocd:geocode', {'prov:label':'OpenCage Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

    input2 = provdoc.entity('dat:' + collec, {prov.model.PROV_LABEL: collecName, prov.model.PROV_TYPE:'ont:DataSet'})

    # output data
    output = provdoc.entity('dat:intermediate', {prov.model.PROV_LABEL:'Intermediate', prov.model.PROV_TYPE:'ont:DataSet'})

    for i, run_id in enumerate(run_ids):
        this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
        query = queries[i]
        querysuffix = query.split("?")[1]
        provdoc.wasAssociatedWith(this_run, this_script)

        provdoc.used(this_run, opencage, startTime, None,\
        {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'json?' + querysuffix})

        provdoc.wasAttributedTo(output, this_script)
        provdoc.wasGeneratedBy(output, this_run, endTime)

        provdoc.wasDerivedFrom(output, opencage, this_run, this_run, this_run)

        provdoc.wasDerivedFrom(output, input2, this_run, this_run, this_run)
        provdoc.used(this_run,input2, startTime, None)

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


def makeProvMBTA(repo, user, run_id, startTime, endTime):
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = provdoc.agent('alg:findnearest', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # input data
    input1 = provdoc.entity('dat:' + 'mbtaStops', {prov.model.PROV_LABEL: "MBTA Stops", prov.model.PROV_TYPE:'ont:DataSet'})

    # output data
    output = provdoc.entity('dat:intermediate', {prov.model.PROV_LABEL:'Intermediate', prov.model.PROV_TYPE:'ont:DataSet'})

    this_run = provdoc.activity('log:a'+run_id, startTime, endTime)

    provdoc.wasAssociatedWith(this_run, this_script)

    provdoc.used(this_run, input1, startTime)

    provdoc.wasAttributedTo(output, this_script)
    provdoc.wasGeneratedBy(output, this_run, endTime)

    provdoc.wasDerivedFrom(output, input1, this_run, this_run, this_run)

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


#print(findDistance((42.3604, -71.0580),    (42.3600, -71.0562)))




