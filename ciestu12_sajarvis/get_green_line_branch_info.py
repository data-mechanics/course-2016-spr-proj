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

# TODO provenance data and recording

repo.logout()
