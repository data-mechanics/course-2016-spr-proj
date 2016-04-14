from geopy.geocoders import GoogleV3
import urllib.request

#geolocator = Nominatim()
geolocator = GoogleV3()

addresses = {}
for ticket in repo['loyuichi.tickets'].find():
	try:
		street_add = ticket["ticket_loc"]
		print(street_add)
		if (street_add not in addresses):
			location = geolocator.geocode(street_add + " Boston")

			print(location)

			if (location):
				zipcode = location.raw['address_components'][-1]["long_name"]
				latitude = location.latitude
				longitude = location.longitude
				addresses[street_add] = {'zip': zipcode, 'location': {'latitude': latitude, 'longitude': longitude}}
				repo['loyuichi.tickets'].update({'_id': meter['_id']}, {'$set': {'zip': zipcode, 'location': {'latitude': latitude, 'longitude': longitude}}})
			else:
				repo['loyuichi.tickets'].update({'_id': meter['_id']}, {'$set': {'zip': addresses[street_add][zipcode], 'location': {'latitude': addresses[street_add][latitude], 'longitude': addresses[street_add][longitude]}}})
	except:
		pass