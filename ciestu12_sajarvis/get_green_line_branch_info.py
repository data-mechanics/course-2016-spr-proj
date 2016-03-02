"""
Inserts hand-crafted JSON representing the branch information for the Green Line
T branches. Includes neighboring stops inbound and outbound, as well as a nice
readable name.
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
    teamname = 'ciestu12_sajarvis'
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(teamname, teamname)

    startTime = datetime.datetime.now()

    # Not using an API here because one did not exist. Created from a subway map
    # published by the MBTA.
    url = 'http://cs-people.bu.edu/sajarvis/datamech/green_line_branch_info.json'
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    print(json.dumps(r, sort_keys=True, indent=2))
    collection = 't_branch_info'
    repo.dropPermanent(collection)
    repo.createPermanent(collection)
    repo['{}.{}'.format(teamname, collection)].insert_many(r)

    endTime = datetime.datetime.now()

    doc = create_prov(startTime, endTime)
    repo.record(doc.serialize()) # Record the provenance document.
    print(doc.get_provn())

    repo.logout()

def create_prov(startTime, endTime):
    # Create provenance data and recording
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bu', 'http://cs-people.bu.edu/sajarvis/datamech/')
    doc.add_namespace('mbta_img', 'http://www.mbta.com/images/')

    # This run has an agent (the script), an entity (the source), and an activity (execution)
    this_script = doc.agent('alg:get_green_line_branch_info', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    resource = doc.entity('bu:green_line_branch_info',
                          {
                              'prov:label':'Green Line Branch Info',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'json'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime)
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, resource, startTime, None,
                {
                    prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})

    # The original sources are entities, too. A map of lines from the MBTA and
    # stop IDs from the GPS file. These are the sources used for the
    # handcrafted data.
    map_resource = doc.entity('mbta_img:subway-spider',
                          {
                              'prov:label':'Map of T Lines and Stops',
                              prov.model.PROV_TYPE:'ont:DataResource',
                              'ont:Extension':'jpg'})
    doc.wasDerivedFrom(resource, map_resource, this_run, this_run, this_run)
    stop_resource = doc.entity('dat:t_stop_locations',
                               {
                                   'prov:label':'Collection with T Stop IDs',
                                   prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasDerivedFrom(resource, stop_resource, this_run, this_run, this_run)

    # Now define entity for the dataset we obtained.
    branch_stops = doc.entity('dat:t_branch_info',
                              {
                                  prov.model.PROV_LABEL:'Green Line Branch Info',
                                  prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(branch_stops, this_script)
    doc.wasGeneratedBy(branch_stops, this_run, endTime)
    doc.wasDerivedFrom(branch_stops, resource, this_run, this_run, this_run)

    return doc

if __name__ == '__main__':
    main()
