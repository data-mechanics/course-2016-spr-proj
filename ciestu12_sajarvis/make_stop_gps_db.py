"""
Create a collection of GPS coordinates for T stops based on .csv data shared
by the MBTA.
"""
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import re

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def main():
    teamname = 'ciestu12_sajarvis'
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(teamname, teamname)

    startTime = datetime.datetime.now()

    def strip(s):
        # Strip Windows carriage returns and extra quotes that came with the MBTA
        # data.
        return re.sub(r'["\n\r]', '', s)

    # Not using an API here because one did not exist. Created from a zipped .csv
    # files shared by the MBTA.
    url = 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/stops.txt'
    response = urllib.request.urlopen(url).read().decode("utf-8")

    # The last entry will be an empty string from last newline, so splice it off.
    all_lines = response.split('\n')[:-1]
    # The headers of the columns exist as the first line of the file.
    headers = [strip(h) for h in all_lines[0].split(',')]
    # Rest of the lines are data. Map them to headers and add to db.
    json_array = []
    for line in all_lines[1:]:
        values = [strip(data) for data in line.split(',')]
        obj = dict(zip(headers, values))
        json_array.append(obj)

    print(json_array)
    collection = "t_stop_locations"
    repo.dropPermanent(collection)
    repo.createPermanent(collection)
    repo['{}.{}'.format(teamname, collection)].insert_many(json_array)

    endTime = datetime.datetime.now()

    # Record the provenance document.
    doc = create_prov(startTime, endTime)
    repo.record(doc.serialize())
    print(doc.get_provn())

    repo.logout()

def create_prov(startTime, endTime):
    '''Add the provenance document for file.'''
    # Create provenance data and recording
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bu_gtfs', 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs')
    doc.add_namespace('mbta_gtfs', 'http://www.mbta.com/uploadedfiles/')

    # This run has an agent (the script), an entity (the source), and an activity (execution)
    this_script = doc.agent('alg:make_stop_gps_db',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    resource = doc.entity('bu_gtfs:stops',
                          {
                              'prov:label':'GPS Locations and IDs of T Stops',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'txt'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime)
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})

    # The original sources are entities, too. The MBTA published a .zip of csv
    # text files including the GPS coordinates of all stops.
    mbta_resource = doc.entity('mbta_gtfs:MBTA_GTFS',
                          {
                              'prov:label':'GTFS Data from MBTA',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'zip'})
    doc.wasDerivedFrom(resource, mbta_resource, this_run, this_run, this_run)

    # Now define entity for the dataset we obtained.
    t_locations = doc.entity('dat:t_stop_locations',
                             {
                                 prov.model.PROV_LABEL:'GPS Locations and IDs of T Stops',
                                 prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(t_locations, this_script)
    doc.wasGeneratedBy(t_locations, this_run, endTime)
    doc.wasDerivedFrom(t_locations, resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
