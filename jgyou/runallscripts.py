'''
runallscripts.py

'''


import random.uniform

exec(open("convertcoordinates.py").read())
exec(open("retrievehospitals.py").read())
exec(open("cleanhospitals.py").read())
exec(open("inputmbta.py").read())
exec(open("findnearest.py").read())
exec(open('../pymongo_dm.py').read())

#exec(open("inputcommcenters.py").read())

# highly unscientific function to generate initial coordinates
# essentially - using a box formed by the following approximate coordinates,
# a series of points are chosen from within the box.
# The box's coordinates were derived from manual retrieval on Google Maps.
# in future versions of this project, points will be chosen using a geojson
# polygon file of Boston neighborhoods to produce points that are less biased
# TR Downtown
# Boston, MA
# 42.351454, -71.055614

# tL Fenway/Kenmore
# Boston, MA
# 42.349171, -71.105396

# BL Southern Mattapan
# Boston, MA
# 42.278593, -71.105052

# BR Lower East Mills / Cedar Grove
# Boston, MA
# 42.279609, -71.053554


def generatePoints(n):
	(topLeftLat, topLeftLon) = (42.349171, -71.105396)
	(topRightLat, topRightLon) = (42.351454, -71.055614)
	(bottomLeftLat, bottomLeftLon) = (42.278593, -71.105052)
	(bottomRightLat, bottomRightLon) = (42.279609, -71.053554)

	x1 = min(topLeftLon, bottomLeftLon)
	x2 = max(topRightLon, bottomRightLon)
	y1 = min(bottomRightLon, bottomLeftLon)
	y2 = max(topRightLat, topLeftLat)

	return [(random.uniform(y1, y2), random.uniform(x1, x2)) for x in range(n)]




client = pymongo.MongoClient()
repo = client.repo

with open("auth.json") as f:
	auth = json.loads(f.read())
	user = auth['user']

	repo.authenticate(user, user)

	startTime = datetime.datetime.now()

	#################

	startingLocations = generatePoints(20)

	for startLocation in startingLocations:
		
		fip = getCensus(startLocation)
		(addr, neigh, zipcode) = getAddress(startLocation)

		distHospital = findClosestCoordinate(repo, user + ".hospitals", startLocation) 



