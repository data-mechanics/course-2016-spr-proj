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

restaurants = repo['ekwivagg_yuzhou7.restaurant'].find()

url = 'http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/stops.txt'
response = urllib.request.urlopen(url).read().decode("utf-8")
f = open('stops.txt', 'w')
f.write(response)

url = 'https://data.cityofboston.gov/resource/hda6-fnsh.json?$select=dbaname&$limit=50000'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("liquor")
repo.createPermanent("liquor")
repo['ekwivagg_yuzhou7.liquor'].insert_many(r)

liquors = repo['ekwivagg_yuzhou7.liquor'].find()

stops = {}
f1 = open('stops.txt', 'r')
for i in range(1, 248):
	line = f1.readline()
	line = line.replace('\"', '')
	if i > 1 and i % 2 == 1 and i < 248:
		stop = line.split(',')
		stops[stop[2]] = [float(stop[4]), float(stop[5])]
	i += 1

stop_loc = []
for stop in stops.keys():
	stop_loc.append((stop, stops[stop][0], stops[stop][1]))

restaurant_loc = []
for restaurant in restaurants:
    location = restaurant['location']
    latitude = location['latitude']
    longitude = location['longitude']
    restaurant_loc.append((restaurant['businessname'], float(latitude), float(longitude)))

liq_names = []
for liquor in liquors:
	if '\xc9' in liquor['dbaname']:
		name = liquor['dbaname'].replace('\xc9', 'e')
	else:
		name = liquor['dbaname']
	liq_names.append(name)

def product(R, S):
    return [(t,u) for t in R for u in S]

def real_aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def aggregate(R, f):
    keys = {r[0] for r in R}
    count = 0
    return [(key1, key2, f([v for (k,v) in R if k[0] == key1])) for (key1, key2) in keys]

restaurant_liq = [(r, rlat, rlong) for ((r, rlat, rlong), (liq)) in product(restaurant_loc, liq_names) if r == liq]
#print(restaurant_liq)

restaurant_liq_stop = []
for restaurant in restaurant_liq:
    min_lat = round(restaurant[1], 2)
    min_long = round(restaurant[2], 2)
    for stop in stop_loc:
        if round(stop[1], 2) == min_lat and round(stop[2], 2) == min_long:
            distance = great_circle((restaurant[1], restaurant[2]), (stop[1], stop[2])).feet
            restaurant_liq_stop.append(((restaurant[0], stop[0]), distance))
#print(restaurant_liq_stop)

closest_stop = aggregate(restaurant_liq_stop, min)
closest_t_stop = []
for pair in closest_stop:
    closest_t_stop.append({'liq': pair[0], 'tstop': pair[1], 'distance':pair[2]})
#print(closest_t_stop)
repo.dropPermanent("closest_stop_liq")
repo.createPermanent("closest_stop_liq")
repo['ekwivagg_yuzhou7.closest_stop_liq'].insert_many(closest_t_stop)

closest_stop_liq = repo['ekwivagg_yuzhou7.closest_stop_liq'].find()

X = []
for pair in closest_stop_liq:
	X.append((pair['tstop'], 1))

Y = real_aggregate(X, sum)
print(Y)