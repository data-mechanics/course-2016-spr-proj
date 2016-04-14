'''
Nicholas Louie, Thomas Hsu
nlouie@bu.edu, thsu@bu.edu
nlouie_thsu8
4/13/16
Boston University Department of Computer Science
CS 591 L1 - Data Mechanics Project 2
Andrei Lapets (lapets@bu.edu)
Datamechanics.io

'''

import requests
import datetime
import json
import pymongo
import prov.model
import uuid
import urllib

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]


def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]


# gets the entire dataset.


def json_get_all(addr, limit=50000, offset=0):
    r = requests.get(addr + "&$limit=" + str(limit) + "&$offset=" + str(offset))
    if len(r.json()) < 1000:
        return r.json()
    else:
        j = r.json()
        offset += limit
        while len(r.json()) == limit:
            r = requests.get(addr + "&$limit=" + str(limit) + '&$offset=' + str(offset))
            j = j + r.json()
            offset += limit
            print(len(j))
        return j


def dict_merge(x, y):
    z = x.copy()
    z.update(y)
    return z


# take collection name (same as dataset name without .json at end)
def insert_to_db(repo, s):
    url = 'https://data-mechanics.s3.amazonaws.com/nlouie_thsu8/data/' + s + '.json'
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)

    # The line below was causing an error.
    #r = json.dumps(r, sort_keys=True, indent=2)

    repo.dropPermanent(s)
    repo.createPermanent(s)

    # Convert to valid JSON dictionary if necessary.
    for i in range(len(r)):
        if type(r[i]) == list:
            d = {}
            d[r[i][0]] = r[i][1]
            r[i] = d

    repo['nlouie_thsu8.' + s].insert_many(r)


# Stolen from Stack Overflow. 
from math import radians, cos, sin, asin, sqrt
def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula 
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	km = 6367 * c
	return km

class mapReduce:
	def __init__(self, dataset):
		self.i = dataset
	
	def map(self, f):
		R = self.i
		self.i = [t for (k,v) in R for t in f(k,v)]
		return self.i

	def reduce(self, f):
		R = self.i
		keys = {k for (k,v) in R}
		self.i = [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]
		return self.i

	def dataset(self):
		return self.i


# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

'''
print 'Reading file. '
crimes_f = open('crimes.json')
crimes_s = crimes_f.read()
crimes_f.close()
crimes = json.loads(crimes_s)
print 'Loaded. '

# Date time from string. 

print 'Filtering. '
pT = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')
crimes_n = [crime for crime in crimes if not (6 < pT(crime["FROMDATE"]).hour < 18)]
print len(crimes_n)

print 'Writing. '
crimes_f = open('crimes_night.json', 'w')
crimes_f.write(json.dumps(crimes_n, indent=4, separators=(',', ': ')))
'''
'''
f = open('lights.json')
lights = json.loads(f.read())
f.close()
f = open('crimes_night.json')
crimes = json.loads(f.read())
f.close()


for i, c in enumerate(crimes[:100]):
	if i % 1000 == 0: 
		print i
	dist = haversine(float(c['Location']['longitude']), float(c['Location']['latitude']), float(lights[0]['Long']), float(lights[0]['Lat']))
	min_l = lights[0]
	for l in lights[1:100]:
		newDist = haversine(float(c['Location']['longitude']), float(c['Location']['latitude']), float(l['Long']), float(l['Lat']))
		if  newDist < dist:
			dist = newDist
			min_l = l
	c['nearestLight'] = l
	c['nearestLight']['distance'] = dist

f = open('crimes_night_light.json', 'w')
f.write(json.dumps(crimes, indent=4, separators=(',', ': ')))
f.close()
'''

c_f = open('crimes_night.txt')
lcd = open('lcd.txt', 'w')
for i, line in enumerate(c_f):
	if i % 10 == 0:
		print i
	if i > 100:
		break
	cid, lat, lon, _, _ = line.split('\t')
	l_f = open('lights.txt')
	dist = -1
	l = -1
	for j, lline in enumerate(l_f):
		if j > 100:
			break
		lid, llat, llong = lline.split('\t')
		newDist = haversine(float(lat), float(lon), float(llat), float(llong))
		if dist == -1 or newDist < dist: 
			dist = newDist
			l = int(lid)
	l_f.close()
	lcd.write('%s\t%s\t%s\n' % (cid, l, str(dist)))
c_f.close()
lcd.close()



# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('nlouie_thsu8', 'nlouie_thsu8')

# must add to database
# insert_to_db(repo,s)

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nlouie_thsu8/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/nlouie_thsu8/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data-mechanics.s3.amazonaws.com/nlouie_thsu8/data')

this_script = doc.agent('alg:script', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

# generate provenance data

resource3 = doc.entity('bdp:7cdf-6fgx', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run3 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':''})
doc.wasAssociatedWith(this_run3, this_script)
doc.used(this_run3, resource3, startTime)

open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()



