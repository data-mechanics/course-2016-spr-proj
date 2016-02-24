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

def request_get_json(source_lat, source_lon, dest_lat, dest_lon):
    '''Make the request to Google Maps for the walking distance from
    source to dest.'''
    url = 'https://maps.googleapis.com/maps/api/distancematrix/json?origins={},{}&destinations={},{}&mode=walking&units=imperial'.format(
        source_lat, source_lon, dest_lat, dest_lon)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    return json.loads(response)

def get_unique_lines():
    '''From the existing t_branch_info collection, get list of unique lines.'''
    # This collection must already exist.
    all_lines = repo['{}.{}'.format(teamname, 't_branch_info')].find({})
    # Set to get only unique
    return set([x['line'] for x in all_lines])

def get_stops_for_line(line):
    '''From the existing t_branch_info collection, all stops on given line.'''
    # This collection must already exist.
    return list(repo['{}.{}'.format(teamname, 't_branch_info')].find({'line':line}))

def main():
    startTime = datetime.datetime.now()

    out_coll = 'green_line_walking_distances'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    for l in get_unique_lines():
        line_stops = []
        for s in [s for s in get_stops_for_line(l)]:
            stop_id = s['stop_id']
            # There should be exactly 1 associated document in the GPS db.
            coords_doc = repo['{}.{}'.format(teamname,  't_stop_locations')].find({'stop_id':stop_id})[0]
            # Append a tuple of the lat,lon, and this stop ID.
            line_stops.append((coords_doc['stop_lat'], coords_doc['stop_lon'], stop_id))

        for r in product(line_stops, line_stops):
            data = request_get_json(r[0][0], r[0][1], r[1][0], r[1][1])
            print(data)
            repo['{}.{}'.format(teamname, out_coll)].insert_one(
                { 'source_id' : r[0][2], \
                'dest_id' : r[1][2], \
                'line' : l, \
                'dist_val_ft' : data['rows'][0]['elements'][0]['distance']['value'], \
                'dist_text' : data['rows'][0]['elements'][0]['distance']['text'], \
                'duration_val_sec' : data['rows'][0]['elements'][0]['duration']['value'], \
                'duration_text' : data['rows'][0]['elements'][0]['duration']['text'] } )

            # We can only have 100 elements per request, and 100 per 10 seconds, so
            # need to rate limit this a bit.
            time.sleep(1.5)

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
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            {
                                prov.model.PROV_TYPE:'ont:Retrieval',
                                'ont:Query':'?origins=<source_lat>,<source_lon>&destinations=<dest_lat>,<dest_lon>&mode=walking&units=imperial'})
    doc.wasAssociatedWith(this_run, this_script)
    doc.used(this_run, goog_resource, startTime)
    doc.used(this_run, coords_resource, startTime)
    doc.used(this_run, branch_resource, startTime)

    # Now define entity for the dataset we obtained.
    distances = doc.entity('dat:green_line_walking_distances',
                           {
                               prov.model.PROV_LABEL:'Green Line Walking Distances Between Stops',
                               prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(distances, this_script)
    doc.wasGeneratedBy(distances, this_run, endTime)
    doc.wasDerivedFrom(distances, goog_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(distances, coords_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(distances, branch_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
