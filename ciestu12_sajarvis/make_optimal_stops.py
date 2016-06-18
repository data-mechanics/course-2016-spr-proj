"""
Calculate the optimal stops based on the calculated score with a weighted
k-means algorithm.

Relies on normal_ppl_sec_util, t_stop_locations, datasets.
"""
import dml
import prov.model
import time
import datetime
import uuid
import urllib.request
import json

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def times(p, c):
    (x,y) = p
    return (x*c, y*c)

def aggregate(s, f):
    keys = {k[0] for k in s}
    return [(key, f([v for k,v in s if k == key])) for key in keys]

def main():
    out_coll = 'optimal_green_stops'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    startTime = datetime.datetime.now()

    # Get all utility measurements.
    sws = repo['{}.{}'.format(teamname, 'normal_ppl_sec_util')].find({})
    ss = repo['{}.{}'.format(teamname, 't_stop_locations')].find({})

    # Get some tuples from the cursors to start with.
    stop_weights = [(s['stop'], s['ppl_secs'], s['line']) for s in sws]
    # Remove duplicate points, for those on multiple branches.
    stops = set([(s['stop_lat'], s['stop_lon'], s['stop_id']) for s in ss])

    # Need to get GPS coordinates added to the stop and weight information.
    joined = [(s,w,b,lat,lon) for s,w,b in stop_weights for lat,lon,si in stops if s == si]

    # Some coords are empty, so filter those out, but where did those
    # come from?? Guess this is why we prov. And project and cast.
    points = [(si, (w, b, float(lat), float(lon))) for (si,w,b,lat,lon) in joined if lat and lon]

    # We can find which x stops to remove from each branch by finding k x fewer
    # than the number of existing.
    for branch, num_stops in aggregate([(l, 1) for s,p,l in stop_weights], sum):

        for k in range(1, num_stops):
            print("{} has {} stops currently, finding {} optimal.".format(
                branch, num_stops, k))
            # Filter points on branch.
            branch_points = [((lat,lon),w) for (si,(w,b,lat,lon)) in points if b == branch]
            # Make the first k points means to start
            means = [(lat,lon) for ((lat,lon),w) in branch_points[:k]]

            for i in range(7): # As an alternative to trying for convergence.
                point_distances = [(a,(b,dist(a,b),w)) for a,w in branch_points for b in means]
                point_min_distances = aggregate(point_distances, lambda x: min(x, key=lambda k:k[1]))
                # Average the points for each mean
                sums = aggregate([(b,times(a,w)) for (a,(b,d,w)) in point_min_distances], plus)
                counts = aggregate([(b,w) for (a,(b,d,w)) in point_min_distances], sum)
                means = sorted([scale(b,d) for a,b in sums for c,d in counts if a == c])

            crds = [{'stop_lat':lat, 'stop_lon':lon} for lat,lon in means]
            elements = {'max': num_stops, 'k':k, 'line':branch, 'coords':crds}
            print(elements)
            repo['{}.{}'.format(teamname, out_coll)].insert_one(elements)

    endTime = datetime.datetime.now()

    # Record the provenance document.
    doc = create_prov(startTime, endTime)
    repo.record(doc.serialize())
    print(doc.get_provn())

    repo.logout()

def create_prov(startTime, endTime):
    '''Create the provenance document for file.'''
    # Create provenance data and recording
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    # This run has an agent (the script), entities (the sources), and an activity (execution)
    this_script = doc.agent('alg:make_optimal_stops',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    normalized_resource = doc.entity('dat:normal_ppl_sec_util',
                                     {
                                         'prov:label':'Normalized People Second Utility Rating',
                                         prov.model.PROV_TYPE:'ont:DataSet'})
    stop_location_resource = doc.entity('dat:t_stop_locations',
                                        {
                                            'prov:label':'Locations for all MBTA Stops',
                                            prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            { prov.model.PROV_LABEL:'Compute Optimal Stop Locations for Each Line',
                              prov.model.PROV_TYPE:'ont:Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, normalized_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.normal_ppl_sec_util.find({})'})
    doc.usage(this_run, stop_location_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.t_stop_locations.find({})'})

    # Now define entity for the dataset we obtained.
    optimal_stops = doc.entity('dat:optimal_green_stops',
                               {
                                   prov.model.PROV_LABEL:'Optimal Green Line Stop Locations for Each Branch',
                                   prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(optimal_stops, this_script)
    doc.wasGeneratedBy(optimal_stops, this_run, endTime)
    doc.wasDerivedFrom(optimal_stops, normalized_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(optimal_stops, stop_location_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
