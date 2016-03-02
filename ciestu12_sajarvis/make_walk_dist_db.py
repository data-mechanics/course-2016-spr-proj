"""
Query Google maps for walking distances between T stops.

Get walking distance and time from every Green Line stop to every other.

It's a combination of GPS coordinates and line information from T data and
distances from Google Maps API.

Relies on these collections already existing in the database:
    t_branch_info
    t_stop_locations

It also takes quite a while, due to rate limiting at the free tier of Google's
API.
"""
import urllib.request
import json
import pymongo
import prov.model
import time
import datetime
import uuid
import sys

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

def product(X, Y):
    '''Cartesian product of sets X and Y'''
    return [(a, b) for a in X for b in Y]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]

def request_get_json(source_lat, source_lon, dest_lat, dest_lon):
    '''Make the request to Google Maps for the walking distance from
    source to dest.'''
    url = 'https://maps.googleapis.com/maps/api/distancematrix/json?origins={},{}&destinations={},{}&mode=walking&units=imperial'.format(
        source_lat, source_lon, dest_lat, dest_lon)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    ret = json.loads(response)
    if(ret['status'] != 'OK'):
        print('Error querying Google. API rate limiting? Exiting with failure code.')
        sys.exit(1)
    return ret

def main():
    startTime = datetime.datetime.now()

    out_coll = 'green_line_walking_distances'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    stops_doc = repo['{}.{}'.format(teamname, 't_branch_info')].find({})
    coords_doc = repo['{}.{}'.format(teamname,  't_stop_locations')].find({})

    # projection to get the values we want for stops and coordinates
    all_stops = [(s['line'], s['stop_id']) for s in stops_doc]
    all_coords = [(c['stop_id'], c['stop_lat'], c['stop_lon']) for c in coords_doc if c['stop_id']]
    # and aggregate all the stops on each branch into an associated list
    line_stops = aggregate(all_stops, list)

    for line, stop_ids in line_stops:
        # a selective product to get the associations between branches and locations
        stop_coords = [(lat, lon, stop_id) for ((s_id),(stop_id,lat,lon)) in product(stop_ids, all_coords) if s_id == stop_id]

        # Could make the assumption that walking from a->b is always the same as
        # b->a, and for now it likely is, but can imagine a scenario where the
        # paths could be different (walking one direction on a branch being
        # blocked by the track, for example). So for simplicity and the
        # flexibility in considering two sides of the track and different
        # optimal paths in different directions, we find distances for the full
        # product.
        for r in product(stop_coords, stop_coords):
            data = request_get_json(r[0][0], r[0][1], r[1][0], r[1][1])
            print(data)
            repo['{}.{}'.format(teamname, out_coll)].insert_one(
                { 'source_id' : r[0][2], \
                'dest_id' : r[1][2], \
                'line' : line, \
                'dist_val_ft' : data['rows'][0]['elements'][0]['distance']['value'], \
                'dist_text' : data['rows'][0]['elements'][0]['distance']['text'], \
                'duration_val_sec' : data['rows'][0]['elements'][0]['duration']['value'], \
                'duration_text' : data['rows'][0]['elements'][0]['duration']['text'] } )

            # We can only have 100 elements per request, and 100 per 10 seconds, so
            # need to rate limit this a bit.
            time.sleep(1.2)

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
    doc.add_namespace('goog', 'https://maps.googleapis.com/')

    # This run has an agent (the script), entities (the sources), and an activity (execution)
    this_script = doc.agent('alg:make_walk_dist_db',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    # Here we use multiple resources, Google's API and two existing collections.
    goog_resource = doc.entity('goog:maps/api/distancematrix/json',
                               {
                                   'prov:label':'Google Distance Matrix API',
                                   prov.model.PROV_TYPE:'ont:Computation'})
    coords_resource = doc.entity('dat:t_stop_locations',
                                 {
                                     'prov:label':'Collection of Locations of T Stops',
                                     prov.model.PROV_TYPE:'ont:DataSet'})
    branch_resource = doc.entity('dat:t_branch_info',
                                 {
                                     'prov:label':'Collection of Green Line Branch Info',
                                     prov.model.PROV_TYPE:'ont:DataSet'})
    # We operated on the data quite a bit too, so make an activity for that in
    # addition to Google's.
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                             startTime, endTime,
                            { prov.model.PROV_LABEL:'Transform Data to Find Matrix of Branch Destinations',
                              prov.model.PROV_TYPE:'Computation' })
    goog_act = doc.activity('log:a'+str(uuid.uuid4()),
                             startTime, endTime,
                            { prov.model.PROV_LABEL:'Use Google Maps API',
                              prov.model.PROV_TYPE:'Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.wasAssociatedWith(goog_act, this_script)
    doc.usage(goog_act, goog_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'?origins=<source_lat>,<source_lon>&destinations=<dest_lat>,<dest_lon>&mode=walking&units=imperial'})
    doc.usage(this_run, coords_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.t_stop_locations.find({})'})
    doc.usage(this_run, branch_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.t_branch_info.find({})'})

    # Now define entity for the dataset we obtained.
    distances = doc.entity('dat:green_line_walking_distances',
                           {
                               prov.model.PROV_LABEL:'Green Line Walking Distances Between Stops',
                               prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(distances, this_script)
    doc.wasGeneratedBy(distances, this_run, endTime)
    doc.wasGeneratedBy(distances, goog_act, endTime)
    doc.wasDerivedFrom(distances, goog_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(distances, coords_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(distances, branch_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
