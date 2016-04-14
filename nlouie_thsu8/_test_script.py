import requests
from datetime import datetime
import json

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





