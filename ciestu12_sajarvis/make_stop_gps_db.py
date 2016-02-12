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

all_lines = response.split('\n')
# The headers of the columns exist as the first line of the file.
headers = [strip(h) for h in all_lines[0].split(',')]
# Rest of the lines are data. Map them to headers and add to db.
json_array = []
for line in all_lines[1:]:
    values = [strip(data) for data in line.split(',')]
    obj = dict(zip(headers, values))
    json_array.append(obj)

collection = "t_stop_locations"
repo.dropPermanent(collection)
repo.createPermanent(collection)
repo['{}.{}'.format(teamname, collection)].insert_many(json_array)

endTime = datetime.datetime.now()

# TODO provenance data and recording

repo.logout()
