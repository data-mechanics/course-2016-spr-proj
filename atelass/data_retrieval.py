'''
This script collects the necessary data from online resources.
'''

import sys
import requests
import pymongo
import json
import time
import prov.model
import datetime
import uuid
from astral import Astral
from haversine import haversine

# Until a library is created, we just use the script directly.
# Path of pymongo_dm.py may need to be changed to ../pymongo_dm.py.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

# Get authorization information.
auth = json.loads(open(sys.argv[1]).read())
mbta_open_development_key = 'wX9NwuHnZU2ToO7GmGR9uw'    # To use in the provenance document
mbta_api_key = auth['services']['mbtadeveloperportal']['key']

# Initialize Astral object for sunrise/sunset data.
a = Astral()
city = a['Boston']

# Retrieve datasets.
print(time.strftime('%I:%M:%S%p') + ': Retrieving crime data...')

crimes_api_endpoint = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json'

# The maximum limit per request is 50,000 records.
limit = 50000
offset = 0

# Request records.
crimes_start_time = datetime.datetime.now()
crimes = []
x = 50000
while x == 50000:
    response = requests.get(crimes_api_endpoint + '?$limit=' + str(limit) + '&$offset=' + str(offset))
    c = response.json()
    crimes += c
    offset += 50000
    x = len(c)

# Get indexes of crimes where lat/lon is 0 (i.e., no specific location given).
indexes = []
for i in range(len(crimes)):
    if crimes[i]['location']['latitude'] == '0.0' or crimes[i]['location']['longitude'] == '0.0':
        indexes.append(i)

# Delete the crimes with no specified location.
num_deleted = 0
for index in indexes:
    del(crimes[index - num_deleted])
    num_deleted += 1

# Get crimes that occurred between sunset and sunrise (i.e., when the streetlights would probably be on).
# First get the indexes of these crimes.
indexes = []
for i in range(len(crimes)):
    crime_date_time = crimes[i]['fromdate'].split('T')
    crime_date = crime_date_time[0].split('-')
    crime_date = datetime.date(int(crime_date[0]), int(crime_date[1]), int(crime_date[2]))
    crime_time = crime_date_time[1].split(':')
    crime_time = datetime.time(int(crime_time[0]), int(crime_time[1]), int(crime_time[2]))

    sunset_time = city.sun(date=crime_date)['sunset']

    # Get crimes from sunset to 1am
    if (crime_time.hour > sunset_time.hour or (crime_time.hour == sunset_time.hour and crime_time.minute >= sunset_time.minute)) or (crime_time.hour < 1):
        indexes.append(i)

# Use these indexes to select the appropriate crimes.
new_crimes = []
for index in indexes:
    new_crimes.append(crimes[index])
crimes = new_crimes

# For some reason, all crimes on and after 2015-06-17 (June 17, 2015) have a time of
# 00:00:00, so remove these since they are inaccurate (I'm assuming here that
# when these records were inputted, they may not have had the times or just
# didn't input them).
index = 0
for i in range(len(crimes)):
    if crimes[i]['fromdate'].split('T')[0] == '2015-06-17':
        index = i
        break

# Now that we have the index of the first crime on June 17, 2015, we can get rid
# of it and all crimes after it.
crimes = crimes[0:index]

# It also appears that there are a large number of crimes on June 16, 2015 that
# have a time of 00:00:00, so I'll also remove these since it's likely that the
# times weren't inputted for the same reasons as those above (it's very unlikely
# that all 110 crimes of these crimes took place at midnight).
indexes = []
for i in range(len(crimes)):
    crime_date_and_time = crimes[i]['fromdate'].split('T')
    if crime_date_and_time[0] == '2015-06-16':
        if crime_date_and_time[1] == '00:00:00':
            indexes.append(i)

num_deleted = 0
for index in indexes:
    del(crimes[index - num_deleted])
    num_deleted += 1

crimes_end_time = datetime.datetime.now()

# Add to MongoDB.
repo.dropPermanent("crimes")
repo.createPermanent("crimes")
repo['atelass.crimes'].insert_many(crimes)

print(time.strftime('%I:%M:%S%p') + ': Completed.')

print(time.strftime('%I:%M:%S%p') + ': Retrieving streetlight data...')

# There doesn't seem to be an API endpoint for this data, so using a URL instead.
streetlights_url = 'https://data.cityofboston.gov/api/views/fbdp-b7et/rows.geojson'

streetlights_start_time = datetime.datetime.now()
response = requests.get('https://data.cityofboston.gov/api/views/fbdp-b7et/rows.geojson')
streetlights_end_time = datetime.datetime.now()

streetlights = response.json()['features']

# Add to MongoDB.
repo.dropPermanent("streetlights")
repo.createPermanent("streetlights")
repo['atelass.streetlights'].insert_many(streetlights)

print(time.strftime('%I:%M:%S%p') + ': Completed.')
print(time.strftime('%I:%M:%S%p') + ': Retrieving stations data...')

# Get Blue, Orange, Red, and B, C, D, E Green Line stop names (only outbound or inbound, not both, since lats and lons seem to be the same).
stopsbyroute_api_endpoint = 'http://realtime.mbta.com/developer/api/v2/stopsbyroute'
routes = ['Blue', 'Orange', 'Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E']
stops = []
stop_names = []

for route in routes:
    response = requests.get(stopsbyroute_api_endpoint + '?api_key=' + mbta_api_key + '&route=' + route)
    route_stops = response.json()['direction'][0]['stop']    # Only getting names of stops in one direction
    for route_stop in route_stops:
        stop_name = route_stop['stop_name'].split(' - ')[0]
        if stop_name not in stop_names:    # Get rid of duplicate stops
            stop_names.append(stop_name)
            stop_location = (route_stop['stop_lat'], route_stop['stop_lon'])
            stops.append({'stop_name': stop_name, 'stop_location': stop_location})

# For each crime, get station closest to it, as long as that station is at most one mile away.
stations = []    # Naming it stations since stops is already taken
count = 0
stations_start_time = datetime.datetime.now()
for crime in crimes:
    if count > 0 and count % 10000 == 0:
        print('\t' + time.strftime('%I:%M:%S%p') + ': Gone through ' + str(count) + ' crimes so far.')
    crime_location = (float(crime['location']['latitude']), float(crime['location']['longitude']))
    closest_stop = {}
    closest_stop_distance = 1.0
    for stop in stops:
        stop_location = stop['stop_location']
        stop_location = (float(stop_location[0]), float(stop_location[1]))
        dist = haversine(crime_location, stop_location, miles=True)
        if dist <= 1.0:
            if dist < closest_stop_distance:
                closest_stop_distance = dist
                closest_stop = {'stop_name': stop['stop_name'], 'distance': closest_stop_distance,\
                                'stop_lat': stop['stop_location'][0], 'stop_lon': stop['stop_location'][1]}
    if closest_stop != {}:    # Ignore crimes that aren't within a 1-mile radius of any stop
        stations.append({'crime_compnos': crime['compnos'], 'station_info': closest_stop})
    count += 1

stations_end_time = datetime.datetime.now()

# Add to MongoDB.
repo.dropPermanent("stations")
repo.createPermanent("stations")
repo['atelass.stations'].insert_many(stations)

print(time.strftime('%I:%M:%S%p') + ': Completed.')


# Provenance information.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/atelass/')
doc.add_namespace('dat', 'http://datamechanics.io/data/atelass/')
doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
doc.add_namespace('log', 'http://datamechanics.io/log#')
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')

this_script = doc.agent('alg:data_retrieval', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

crimes_resource = doc.entity('bdp:7cdf-6fgx', {'prov:label': 'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
streetlights_resource = doc.entity('bdp:7hu5-gg2y', {'prov:label': 'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
stopsbyroute_resource = doc.entity('mbta:stopsbyroute', {'prov:label': 'Stops by Route', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

# Crimes provenance information.
get_crimes = doc.activity('log:a'+str(uuid.uuid4()), crimes_start_time, crimes_end_time)
doc.wasAssociatedWith(get_crimes, this_script)
doc.usage(get_crimes, crimes_resource, crimes_start_time, None, {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':('?$limit=' + str(limit) + '&$offset=' + str(offset))})

crimes = doc.entity('dat:crimes', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crimes, this_script)
doc.wasGeneratedBy(crimes, get_crimes, crimes_end_time)
doc.wasDerivedFrom(crimes, crimes_resource, get_crimes)

# Streetlights provenance information
get_streetlights = doc.activity('log:a'+str(uuid.uuid4()), streetlights_start_time, streetlights_end_time)
doc.wasAssociatedWith(get_streetlights, this_script)
doc.used(get_streetlights, streetlights_resource, streetlights_start_time)

streetlights = doc.entity('dat:streetlights', {prov.model.PROV_LABEL:'Streetlights', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(streetlights, this_script)
doc.wasGeneratedBy(streetlights, get_streetlights, streetlights_end_time)
doc.wasDerivedFrom(streetlights, streetlights_resource, get_streetlights)

# Stations provenance information
get_stations = doc.activity('log:a'+str(uuid.uuid4()), stations_start_time, stations_end_time)
doc.wasAssociatedWith(get_stations, this_script)
doc.used(get_stations, stopsbyroute_resource, stations_start_time)
doc.used(get_stations, crimes, stations_start_time)

stations = doc.entity('dat:stations', {prov.model.PROV_LABEL:'Stations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(stations, this_script)
doc.wasGeneratedBy(stations, get_stations, stations_end_time)
doc.wasDerivedFrom(stations, stopsbyroute_resource, get_stations)
doc.wasDerivedFrom(stations, crimes, get_stations)

repo.record(doc.serialize())
open('plan.json', 'w').write(json.dumps(json.loads(doc.serialize()), indent=4))
#print(doc.get_provn())
repo.logout()
