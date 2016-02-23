#get api key for google geolocation
import json
import googlemaps
import apitest

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

ticketsUrl = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json' + '?$$app_token=' + data["api_key"]
# generate list of addresses
(rawAddr, formatted) = apitest.request(ticketsUrl)

def generateAddr(json_obj):
	return [elem['ticket_loc'] for elem in json_obj]

#print(generateAddr(rawAddr))

def locationQueryHelper(addr):
	queryAddr = addr + ', Boston, Ma'
	geocode_result = gmaps.geocode(queryAddr)
	return str(geocode_result[0]['geometry']['location_type'])

def locationQuery(addr_list):
	return [locationQueryHelper(elem) for elem in addr_list if locationQueryHelper(elem) != 'APPROXIMATE']







