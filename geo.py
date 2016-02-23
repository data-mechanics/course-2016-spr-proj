#get api key for google geolocation
import json
import googlemaps

with open('auth.json') as credentials:
	data = json.load(credentials)

gmaps = googlemaps.Client(key=data['maps_api_key']) 

'''
make sure to create a function that appends 
(Boston, Ma) to the geolocation request.
also try and find a standard to normalize street numbers
of possible. Specifically in regards to how 
we deal with addresses with no numbers given
'''

# generate list of addresses

def locationQueryHelper(addr):
	queryAddr = addr + ', Boston, Ma'
	geocode_result = gmaps.geocode(queryAddr)
	return str(geocode_result[0]['geometry']['location_type'])

def locationQuery(addr_list):
	return [locationQueryHelper(elem) for elem in addr_list if locationQueryHelper(elem) != 'APPROXIMATE']







