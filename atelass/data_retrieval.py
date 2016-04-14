import sys
import requests
import json
import pymongo
import prov.model
import time

# Until a library is created, we just use the script directly.
# Path of pymongo_dm.py may need to be changed to ../pymongo_dm.py.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

# Get authorization information.
auth = json.loads(open(sys.argv[1]).read())
mbta_api_key = auth['services']['mbtadeveloperportal']['key']

# Approximate sunset/sunrise times for each month in 2015 (source: timeanddate.com).
# Used the sunset/sunrise time for the 15th day of each month.
# Assume approximately the same times in 2012-2014 as well.
sunset_sunrise_times = {1: {'sunset': '16:37', 'sunrise': '07:11'},\
                        2: {'sunset': '17:16', 'sunrise': '06:41'},\
                        3: {'sunset': '18:50', 'sunrise': '06:57'},\
                        4: {'sunset': '19:26', 'sunrise': '06:04'},\
                        5: {'sunset': '19:59', 'sunrise': '05:23'},\
                        6: {'sunset': '20:23', 'sunrise': '05:07'},\
                        7: {'sunset': '20:19', 'sunrise': '05:21'},\
                        8: {'sunset': '19:46', 'sunrise': '05:51'},\
                        9: {'sunset': '18:54', 'sunrise': '06:24'},\
                       10: {'sunset': '18:03', 'sunrise': '06:57'},\
                       11: {'sunset': '16:22', 'sunrise': '06:35'},\
                       12: {'sunset': '16:12', 'sunrise': '07:06'}}


# Retrieve datasets.
print('Beginning to retrieve the crime data at ' + time.strftime('%I:%M:%S%p'))

crimes_api_endpoint = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json'

# The maximum limit per request is 50,000 records.
limit = 50000
offset = 0

# Request records.
crimes = []
x = 50000
while x == 50000:
    response = requests.get(crimes_api_endpoint + '?$limit=' + str(limit) + '&$offset=' + str(offset))
    c = response.json()
    crimes += c
    offset += 50000
    x = len(c)

# Get indexes of crimes where lat/long is 0 (i.e., no specific location given).
indexes = []
for i in range(len(crimes)):
    if crimes[i]['location']['latitude'] == '0.0' or crimes[i]['location']['longitude'] == '0.0':
        indexes.append(i)

# Delete the crimes with no specified location.
num_deleted = 0
for i in range(len(crimes)):
    if i in indexes:
        del(crimes[i - num_deleted])
        num_deleted += 1

# Get crimes that occurred between sunset and sunrise (i.e., when the streetlights would probably be on).
# First get the indexes of these crimes.
indexes = []
for i in range(len(crimes)):
    crime_month= int(crimes[i]['month'])
    crime_time = crimes[i]['fromdate'].split('T')[1]
    crime_hour = int(crime_time[0:2])
    crime_minute = int(crime_time[3:5])
    sunset_hour = int(sunset_sunrise_times[crime_month]['sunset'][0:2])
    sunset_minute = int(sunset_sunrise_times[crime_month]['sunset'][3:5])
    sunrise_hour = int(sunset_sunrise_times[crime_month]['sunrise'][0:2])
    sunrise_minute = int(sunset_sunrise_times[crime_month]['sunrise'][3:5])
    if crime_hour >= sunset_hour or crime_hour <= sunrise_hour:
        if (crime_hour == sunset_hour and crime_minute >= sunset_minute) or (crime_hour == sunrise_hour and crime_minute <= sunrise_minute):
            indexes.append(i)

# Use these indexes to select the appropriate crimes.
new_crimes = []
for i in range(len(crimes)):
    if i in indexes:
        new_crimes.append(crimes[i])
crimes = new_crimes

# Add to MongoDB.
repo.dropPermanent("crimes")
repo.createPermanent("crimes")
repo['atelass.crimes'].insert_many(crimes)

print('Finished retrieving crime data at ' + time.strftime('%I:%M:%S%p'))
print()
print('Beginning to retrieve the streetlight data at ' + time.strftime('%I:%M:%S%p'))

# There doesn't seem to be an API endpoint for this data, so using a URL instead.
streetlights_url = 'https://data.cityofboston.gov/api/views/fbdp-b7et/rows.geojson'

response = requests.get('https://data.cityofboston.gov/api/views/fbdp-b7et/rows.geojson')

streetlights = response.json()['features']

# Add to MongoDB.
repo.dropPermanent("streetlights")
repo.createPermanent("streetlights")
repo['atelass.streetlights'].insert_many(streetlights)

print('Finished retrieving streetlight data at ' + time.strftime('%I:%M:%S%p'))
print()
print('Beginning to get stops data at ' + time.strftime('%I:%M:%S%p'))

# Get Blue, Orange, Red, and B, C, D, E Green Line stop names (only outbound or inbound, not both, since lats and lons seem to be the same.
stopsbyroute_api_endpoint = 'http://realtime.mbta.com/developer/api/v2/stopsbyroute'
routes = ['Blue', 'Orange', 'Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E']
stops = []
for route in routes:
    response = requests.get(stopsbyroute_api_endpoint + '?api_key=' + mbta_api_key + '&route=' + route)
    route_stops = response.json()
    for i in range(len(route_stops['direction'][0]['stop'])):
        stop = route_stops['direction'][0]['stop'][i]['stop_name']
        # Edit format of results (e.g. for 'Griggs Street - Outbound', we onyl want 'Griggs Street').
        stop = stop.split('-')[0][0:-1]
        stops.append(stop)
stops = list(set(stops))    # Get rid of duplicate stops.

# Get stops corresponding to crime locations.
stopsbylocation_api_endpoint = 'http://realtime.mbta.com/developer/api/v2/stopsbylocation'

stations = []
count = 0
for crime in crimes:
    if count % 100 == 0:
        time.sleep(5)
    lat = crime['location']['latitude']
    lon = crime['location']['longitude']
    response = requests.get(stopsbylocation_api_endpoint + '?api_key=' + mbta_api_key + '&lat=' + lat + '\
&lon=' + lon)
    stop = response.json()['stop']
    # Query returns 15 stops, so take the first one, which is the closest to the given latitude and longitude. If no stop is close to it, return None
    if stop == []:
        stations.append({crime['compnos']: None})
    else:
        stations.append({crime['compnos']: stop[0]})
    count += 1

# Add to MongoDB.
repo.dropPermanent("stations")
repo.addPermanent("stations")
repo['atelass.stations'] = insert_many(stations)

print('Finished getting stops data at ' + time.strftime('%I:%M:%S%p'))
