import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
from geopy.distance import great_circle

# Until a library is created, we just use the script directly.
exec(open('pymongo_dm.py').read())

# Set up the database connection.
auth = open('auth.json', 'r')
cred = json.load(auth)
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?incident_type_description=larceny+from+motor+vehicle&$select=location,compnos&$limit=50000'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("larceny")
repo.createPermanent("larceny")
repo['ekwivagg_yuzhou7.larceny'].insert_many(r)

larcenies = repo['ekwivagg_yuzhou7.larceny'].find()

url = 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/stops.txt'
response = urllib.request.urlopen(url).read().decode("utf-8")
f = open('stops.txt', 'w')
f.write(response)

count = 1
stops = {}
f1 = open('stops.txt', 'r')
for i in range(1, 248):
	line = f1.readline()
	line = line.replace('\"', '')
	if i > 1 and i % 2 == 1 and i < 248:
		stop = line.split(',')
		stops[stop[2]] = [float(stop[4]), float(stop[5])]
	i += 1
#print(stops)

def product(R, S):
    return [(t,u) for t in R for u in S]

def real_aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def aggregate(R, f):
    keys = {r[0] for r in R}
    count = 0
    return [(key1, key2, f([v for (k,v) in R if k[0] == key1])) for (key1, key2) in keys]

stop_loc = []
for stop in stops.keys():
	stop_loc.append((stop, stops[stop][0], stops[stop][1]))

count = 0
larceny_loc = []
for larceny in larcenies:
    location = larceny['location']
    latitude = location['latitude']
    longitude = location['longitude']
    larceny_loc.append((count, float(latitude), float(longitude)))
    count += 1
#print(larceny_loc)

larceny_stop = []
for larceny in larceny_loc:
    min_lat = round(larceny[1], 2)
    min_long = round(larceny[2], 2)
    for stop in stop_loc:
        if round(stop[1], 2) == min_lat and round(stop[2], 2) == min_long:
            distance = great_circle((larceny[1], larceny[2]), (stop[1], stop[2])).feet
            larceny_stop.append(((larceny[0], stop[0]), distance))
print(larceny_stop)

closest_stop = aggregate(larceny_stop, min)
closest_t_stop = []
for pair in closest_stop:
    closest_t_stop.append({'larceny': pair[0], 'tstop': pair[1], 'distance':pair[2]})
#print(closest_t_stop)
repo.dropPermanent("closest_stop_larceny")
repo.createPermanent("closest_stop_larceny")
repo['ekwivagg_yuzhou7.closest_stop_larceny'].insert_many(closest_t_stop)

closest_stop_larceny = repo['ekwivagg_yuzhou7.closest_stop_larceny'].find()

X = []
for pair in closest_stop_larceny:
	X.append((pair['tstop'], 1))

Y = real_aggregate(X, sum)
print(Y)