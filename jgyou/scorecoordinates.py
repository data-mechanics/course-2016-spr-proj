'''
scorecoordinates.py
produces scores

note that provenance for each data source is generated either
in the original retrieval script (e.g. retrievehospitals.py)
or in the findnearest.py or convertcoordinates.py scripts
'''
from random import uniform
from statistics import median

# supplementary functions
exec(open("convertcoordinates.py").read())  
exec(open("findnearest.py").read())   
exec(open("generatemiscprov.py").read())
exec(open('../pymongo_dm.py').read())


# highly unscientific function to generate initial coordinates
# essentially - using a box formed by the following approximate coordinates,
# a series of points are chosen from within the box.
# The box's coordinates were derived from manual retrieval on Google Maps.
# in future versions of this project, points will be chosen using a geojson
# polygon file of Boston neighborhoods to produce points that are less biased
# TR Downtown
# Boston, MA
# 42.351454, -71.055614

# tL Fenway/Kenmore
# Boston, MA
# 42.349171, -71.105396

# BL Southern Mattapan
# Boston, MA
# 42.278593, -71.105052

# BR Lower East Mills / Cedar Grove
# Boston, MA
# 42.279609, -71.053554


def generatePoints(n):
    (topLeftLat, topLeftLon) = (42.349171, -71.105396)
    (topRightLat, topRightLon) = (42.351454, -71.055614)
    (bottomLeftLat, bottomLeftLon) = (42.278593, -71.105052)
    (bottomRightLat, bottomRightLon) = (42.279609, -71.053554)

    x1 = min(topLeftLon, bottomLeftLon)
    x2 = max(topRightLon, bottomRightLon)
    y1 = min(bottomRightLat, bottomLeftLat)
    y2 = max(topRightLat, topLeftLat)

    # 5-decimal place lat-long coordinates randomly created
    return [(round(uniform(y1, y2), 5), round(uniform(x1, x2), 5)) for x in range(n)]

# wheelchair status from mbta is:
# 0 = no wheelchair-accessibility
# 1 = wheelchair accessibility
# 2 = unknown accessibility status
def assignStopWeight(wheelchairstatus):
    if wheelchairstatus == 0 or wheelchairstatus == 2:
        return 1.0
    return 0.9

def make_provdoc(repo, user, run_id, startTime, endTime):
    provdoc = prov.model.ProvDocument()
    provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
    provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
    provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    # activity = invocation of script, agent = script, entity = resource
    # agent
    this_script = provdoc.agent('alg:scorecoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    this_run = provdoc.activity('log:a'+run_id, startTime, endTime)
    resource = provdoc.entity('dat:intermediate', {prov.model.PROV_LABEL:'Intermediate', prov.model.PROV_TYPE:'ont:DataSet'})
    output1 = provdoc.entity('dat:scores', {prov.model.PROV_LABEL:'Distance Scores', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension': 'json'})

    provdoc.wasAssociatedWith(this_run, this_script)
    provdoc.used(this_run, resource, startTime)

    output = output1
    provdoc.wasAttributedTo(output, this_script)
    provdoc.wasGeneratedBy(output, this_run, endTime)
    provdoc.wasDerivedFrom(output, resource, this_run, this_run, this_run)


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


client = pymongo.MongoClient()
repo = client.repo

with open("auth.json") as f:
    auth = json.loads(f.read())
    user = auth['user']

    repo.authenticate(user, user)

    startTime = datetime.datetime.now()

    #################

    startingLocations = generatePoints(30)

    weights = [1, 3, 6]

    allscores = []

    for startLocation in startingLocations:

        (startlat, startlon) = startLocation
        
        fip = getCensus(repo, user, startLocation)  # problematic line
        
        (addr, neigh, zipcode) = getAddress(repo, user, startLocation)

        if zipcode == -1:
            # zipcode not found by geocoder due to incomplete data, look for zipcode based on neighborhood
            continue

        
        # find nearest hospital
        distHospital = findClosestCoordinate(repo, user, user + ".hospitals", "Hospitals", startLocation)
    
        # when scoring mbta stops, "rewards" stops that have wheelchair access
        nearerStops = boundedRadiusMBTA(repo, user, user + ".mbtaStops", startLocation, 3.0)
        if len(nearerStops) != 0:
            medianMBTA = median([b*assignStopWeight(f) for (a,b, c, d, e, f) in nearerStops])
        else:
            medianMBTA = 3.0

        # find the nearest service center
        distCenter = findClosestCoordinate(repo, user, user + '.servicecenters', "Service Centers", startLocation)

        # calculate overall weighted distance score
        score = weights[0]*distHospital + weights[2]*medianMBTA + weights[1]*distCenter

        allscores.append( {"address": addr, "census_block": fip, "score": score, "longitude": startlon, "latitude": startlat})

        # output score data
    with open('scores.json', 'w') as output_scores:
        output_scores.write(json.dumps(allscores, indent=4))

        output_scores.close()

        with open('scoresgeo.json', 'w') as output_scores2:
            output_scores2.write('scoresjs = ' + json.dumps(allscores, indent=4) + ";")

            output_scores2.close()

            run_id = str(uuid.uuid4())
            make_provdoc(repo, user, run_id, startTime, endTime)
            make_provdoc(repo, user, run_id, None, None)

            repo.logout()
