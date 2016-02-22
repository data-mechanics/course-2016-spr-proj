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

geocode_result = gmaps.geocode('10 beacon st, Boston, Ma')

print(geocode_result)