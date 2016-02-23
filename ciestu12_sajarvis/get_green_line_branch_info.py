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
collection = "t_branch_info"
repo.dropPermanent(collection)
repo.createPermanent(collection)
repo['{}.{}'.format(teamname, collection)].insert_many(r)

endTime = datetime.datetime.now()

# Create provenance data and recording
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bu', 'http://cs-people.bu.edu/sajarvis/datamech/')

this_script = doc.agent('alg:get_green_line_branch_info', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bu:green_line_branch_info', {'prov:label':'Green Line Branch Info', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
# No additional query needed for this data
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

branch_stops = doc.entity('dat:branch_stops', {prov.model.PROV_LABEL:'Green Line Branch Stops', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(boardings, this_script)
doc.wasGeneratedBy(branch_stops, this_run, endTime)
doc.wasDerivedFrom(branch_stops, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
print(doc.get_provn())

repo.logout()
