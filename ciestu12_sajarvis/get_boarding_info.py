"""
Inserts hand-crafted JSON representing the boarding counts of the Green Line T
during a weekday.
"""
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def main():
    '''Wrapper to prevent all script from running when imported.'''

    teamname = 'ciestu12_sajarvis'
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(teamname, teamname)

    startTime = datetime.datetime.now()

    # Not using an API here because one did not exist, taken from the "Blue Book"
    # published by the MBTA.
    url = 'http://cs-people.bu.edu/sajarvis/datamech/green_line_boarding.json'
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    print(json.dumps(r, sort_keys=True, indent=2))
    collection = 'green_line_boarding_counts'
    repo.dropPermanent(collection)
    repo.createPermanent(collection)
    repo['{}.{}'.format(teamname, collection)].insert_many(r)

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
    doc.add_namespace('bu', 'http://cs-people.bu.edu/sajarvis/datamech/')
    doc.add_namespace('mbta_docs', 'http://www.mbta.com/uploadedfiles/documents/')

    # This run has an agent (the script), an entity (the source), and an activity (execution)
    this_script = doc.agent('alg:get_boarding_info',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    resource = doc.entity('bu:green_line_boarding',
                          {
                              'prov:label':'Green Line Boarding Counts',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'json'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime)
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, resource, startTime, None,
            {
                prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})

    # The original sources are entities, too. The BlueBook data from the MBTA
    # stop IDs from the GPS collection. These are the sources used for the
    # handcrafted data.
    mbta_resource = doc.entity('mbta_docs:2014%20BLUEBOOK%2014th%20Edition',
                          {
                              'prov:label':'PDF of Green Line Boarding Counts',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'pdf'})
    doc.wasDerivedFrom(resource, mbta_resource, this_run, this_run, this_run)
    stop_resource = doc.entity('dat:t_stop_locations',
                               {
                                   'prov:label':'Collection with T Stop IDs',
                                   prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasDerivedFrom(resource, stop_resource, this_run, this_run, this_run)

    # Now define entity for the dataset we obtained.
    boardings = doc.entity('dat:green_line_boarding_counts',
                           {
                               prov.model.PROV_LABEL:'Green Line Boarding Counts',
                               prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(boardings, this_script)
    doc.wasGeneratedBy(boardings, this_run, endTime)
    doc.wasDerivedFrom(boardings, resource, this_run, this_run, this_run)

    return doc


if __name__ == "__main__":
    main()
