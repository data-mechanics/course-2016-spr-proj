from geopy.geocoders import Nominatim

geolocator = Nominatim()

# Open the file for interfacing with DB
exec(open('../pymongo_dm.py').read())

# Set up the db connection
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jtsliu_kmann', 'jtsliu_kmann')

#Parse through dataases and put it into a temporary list.
def getCollection(dbName):
	temp = []
	for elem in repo['jtsliu_kmann.' + dbName].find({}):
		temp.append(elem)
	return temp

crime_data = getCollection('crime')

count = 0
for x in crime_data:
	if  count == 5:
		break
	lng = x['location']['coordinates'][0]
	lat = x['location']['coordinates'][1]
	print(lng, lat)
	location = geolocator.reverse(str(lat) + ", " + str(lng))
	count += 1
	print(location.raw['address']['postcode'], type(location.raw['address']['postcode']))


