"""
Use the 'green_line_walking_distances' to figure out the time and distance
to the nearest neighbor stop on the line, for each stop on the line.

Requires the 'green_line_walking_distances' collection.
"""
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def map(R, f):
    '''Map set R according to function f. R is a set of tuples.'''
    return [t for (k, v) in R for t in f(k, v)]

def reduce(R, f):
    '''Reduce the set R with function f.'''
    keys = {k for (k, v) in R}
    return [f(k, [v for (k1, v) in R if k == k1]) for k in keys]

def main():
    teamname = 'ciestu12_sajarvis'
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(teamname, teamname)

    startTime = datetime.datetime.now()

    # Transformation on the 'green_line_walking_distances' collection
    all_stops = repo['{}.{}'.format(teamname, 'green_line_walking_distances')].find({})
    # Make a list of tuples to start, with all the information we'd
    # like to consider during operation and for the resulting data set.
    stops = [(s['source_id'], (s['line'], s['dest_id'], s['dist_val_ft'], s['duration_val_sec'])) for s in all_stops]

    # Map together all results for each stop on each particular line.
    distances = map(stops, lambda k,v: [((k,v[0]), v)])

    # Reduce on the minimum *time* distance walking to the other stops on
    # the line. Second lambda function serves as the key to minimize (time).
    shortests = reduce(distances,
                       lambda k,vs: (k[0], min([v for v in vs if v[1] != k[0]], key = lambda t : t[3])))

    # Put the resulting list of tuples into a JSON-friendly dictionary.
    elements = [{'stop':k, 'line':v[0], 'nearest':v[1], 'distance_ft':v[2], 'time_sec':v[3]} for k,v in shortests]
    print(elements)

    nearest_coll = 'nearest_stops'
    repo.dropPermanent(nearest_coll)
    repo.createPermanent(nearest_coll)
    repo['{}.{}'.format(teamname, nearest_coll)].insert_many(elements)

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
    this_script = doc.agent('alg:make_nearest_stops',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    dist_resource = doc.entity('dat:green_line_walking_distances',
                               {
                                   'prov:label':'Walking Distances Between All Green Line Stops',
                                   prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            { prov.model.PROV_LABEL:'Find Nearest Neighbor Stops',
                              prov.model.PROV_TYPE:'Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, dist_resource, startTime,None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.green_line_walking_distances.find({})'})

    # Now define entity for the dataset we obtained.
    nearests = doc.entity('dat:nearest_stops',
                          {
                              prov.model.PROV_LABEL:'Nearest Neighboring Stops For Green Line Stops',
                              prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(nearests, this_script)
    doc.wasGeneratedBy(nearests, this_run, endTime)
    doc.wasDerivedFrom(nearests, dist_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
