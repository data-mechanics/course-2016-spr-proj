import urllib.request
import json
import pymongo
import prov.model
import random
import uuid
import datetime

def run(repo):

    # Future work... No zip code with a large dataset of permits converges 
    # in less than the maximum number of iterations: 100.
    # Perhaps tolerances can be relaxed, or the algorithm improved to converge
    # faster than it currently does.

    startTime = datetime.datetime.now()

    repo.dropPermanent("residential_centers")
    repo.createPermanent("residential_centers")
    repo.dropPermanent("commercial_centers")
    repo.createPermanent("commercial_centers")

    zipcodes = repo.djmcc_jasper.assessments_2014.distinct('zipcode')

    for zipcode in zipcodes:

        print("\nRunning KMeans on " + zipcode + "...")
        print("Finding all 2014 Property Assessments...")

        assessments_2014 = list(repo.djmcc_jasper.assessments_2014.find({'zipcode':zipcode}))

        print("Finding all 2015 Property Assessments...")

        assessments_2015 = list(repo.djmcc_jasper.assessments_2015.find({'zipcode':zipcode}))

        print("Finding all 2014-2015 Approved Permits...")

        permits = list(repo.djmcc_jasper.approved_permits.find({'zip':zipcode}))

        P_res   = []
        P_com   = []
        matched = []

        print("Running cross check against 2014 data...")

        for permit in permits:
            if permit['parcel_id'] not in matched:
                for assessment in assessments_2014:
                    if permit['parcel_id'] == assessment['parcel_id']:
                        matched.append(permit['parcel_id'])
                        lat = float(permit['location']['latitude'])
                        lon = float(permit['location']['longitude'])
                        if assessment['ptype'] >= "100" and assessment['ptype'] < "200":

                            P_res.append((lat, lon))
                        elif assessment['ptype'] >= "300" and assessment['ptype'] < "400":
                            P_com.append((lat, lon))

        print("Running cross check against 2015 data...")
        
        for permit in permits:
            if permit['parcel_id'] not in matched:
                for assessment in assessments_2015:
                    if permit['parcel_id'] == assessment['pid']:
                        matched.append(permit['parcel_id'])
                        lat = float(permit['location']['latitude'])
                        lon = float(permit['location']['longitude'])
                        if assessment['ptype'] >= "100" and assessment['ptype'] < "200":
                            P_res.append((lat, lon))
                        elif assessment['ptype'] >= "300" and assessment['ptype'] < "400":
                            P_com.append((lat, lon))

        print("Number of residential permits: " + str(len(P_res)))
        print("Number of commercial permits: " + str(len(P_com)))

        if len(P_res) >= 10:
            print("Storing residential centers...")
            storeResults(P_res, repo.djmcc_jasper.residential_centers, zipcode)
        else:
            print("Not enough data, skipping the algorithm for residential centers...")
        
        if len(P_com) >= 10:
            print("Storing commercial centers...")
            storeResults(P_com, repo.djmcc_jasper.commercial_centers, zipcode)
        else:
            print("Not enough data, skipping the algorithm for commercial centers...")

    endTime = datetime.datetime.now()

    # Create the provenance document describing everything happening
    # in this script. Each run of the script will generate a new
    # document describing that invocation event. This information
    # can then be used on subsequent runs to determine dependencies
    # and "replay" everything. The old documents will also act as a
    # log.

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')

    # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('geo', 'http://api.geonames.org/')

    # Agent

    this_script = doc.agent('alg:kmeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # Entity: Datasets

    dataset_2014    = doc.entity('dat:assessments_2014', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    dataset_2015    = doc.entity('dat:assessments_2015', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    dataset_permits = doc.entity('dat:approved_permits', {'prov:label':'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    resource_geonames = doc.entity('geo:findNearestAddressJSON', {'prov:label':'Nearest Address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

    # Activity: Retrieval

    retrieve_2014    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
    retrieve_2015    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
    retrieve_permits = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(retrieve_2014, this_script)
    doc.wasAssociatedWith(retrieve_2015, this_script)
    doc.wasAssociatedWith(retrieve_permits, this_script)

    doc.used(retrieve_2014, dataset_2014, startTime)
    doc.used(retrieve_2015, dataset_2015, startTime)
    doc.used(retrieve_permits, dataset_permits, startTime)

    # Activity: Query

    query_geonames = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?lat=<LAT>&lng=<LNG>&username=djmcc'})
    doc.wasAssociatedWith(query_geonames, this_script)
    doc.used(query_geonames, resource_geonames, startTime)

    # Activity: K-Means

    kmeans = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    doc.wasAssociatedWith(kmeans, this_script)

    doc.used(kmeans, dataset_2014, startTime)
    doc.used(kmeans, dataset_2015, startTime)
    doc.used(kmeans, dataset_permits, startTime)
    doc.used(kmeans, resource_geonames, startTime)

    # Entity: Derived Datasets

    residential_centers = doc.entity('dat:residential_centers', {prov.model.PROV_LABEL:'Residential Centers', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(residential_centers, this_script)
    doc.wasGeneratedBy(residential_centers, kmeans, endTime)
    doc.wasDerivedFrom(residential_centers, dataset_2014, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(residential_centers, dataset_2015, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(residential_centers, dataset_permits, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(residential_centers, resource_geonames, kmeans, kmeans, kmeans)

    commercial_centers = doc.entity('dat:commercial_centers', {prov.model.PROV_LABEL:'Commercial Centers', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(commercial_centers, this_script)
    doc.wasGeneratedBy(commercial_centers, kmeans, endTime)
    doc.wasDerivedFrom(commercial_centers, dataset_2014, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(commercial_centers, dataset_2015, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(commercial_centers, dataset_permits, kmeans, kmeans, kmeans)
    doc.wasDerivedFrom(commercial_centers, resource_geonames, kmeans, kmeans, kmeans)

    repo.record(doc.serialize()) # Record the provenance document.
    # print(json.dumps(json.loads(doc.serialize()), indent=4))
    # print(doc.get_provn())

def storeResults(P, collection, zipcode):

    if len(P) < 25:
        (means, iterations) = kmeans(P, 1)
        print("Iterations: " + str(iterations) + "...")
    elif len(P) < 50:
        (means, iterations) = kmeans(P, 2)
        print("Iterations: " + str(iterations) + "...")
    else:
        (means, iterations) = kmeans(P, 3)
        print("Iterations: " + str(iterations) + "...")

    for i in range(0, len(means)):
        (lat, lon) = means[i]
        url = "http://api.geonames.org/findNearestAddressJSON?lat=" + \
              str(lat) + "&lng=" + str(lon) + "&username=djmcc"
        result = urllib.request.urlopen(url).read().decode('utf-8')
        r = json.loads(result)
        r['parent_zip'] = zipcode
        collection.insert_one(r)

# All code below this point was derived from the course notes for
# CS 591L Spring 2016 and used with permission by Andrei Lapets.

def kmeans(P, n):

    n_max = len(P) - 1              # n_max is the number of obs in P less 1
    M = []                          # init means list

    # select points at random from P for the init means
    for i in range(0, n):
        M.append(P[random.randint(0, n_max)])

    M_prev = []                     # means list from the previous iteration
    iterations = 0                  # counter
    max_iterations = 100            # limit

    # Lloyd's Algorithm
    while M_prev != M and iterations < max_iterations:
        M_prev = M

        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        iterations += 1
    
    return (sorted(M), iterations)

# Cross Product
def product(R, S):
    return [(t,u) for t in R for u in S]

# Reduction over distinct keys using the function f
def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]

# Euclidean distance (squared)
def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

# Returns (xt, yt) where xt = sum(x in args) and yt = sum(y in args)
def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

# Simple division
def scale(p, c):
    (x,y) = p
    return (round(x/c, 5), round(y/c, 5))

#EOF