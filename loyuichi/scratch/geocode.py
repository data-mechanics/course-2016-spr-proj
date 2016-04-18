#from geopy.geocoders import Nominatim
from geopy.geocoders import GoogleV3
import urllib.request

#geolocator = Nominatim()
geolocator = GoogleV3()

#    location = geolocator.reverse("42.3511, -71.0603")
add = "113 HAVRE ST" + " Boston"
location = geolocator.geocode(add)
#location = geolocator.reverse("52.509669, 13.376294")
print(location[0])
print(type(location[0]))
#print(len(location[0]))
print(location.raw['address_components'][-1]["long_name"])
print((location.latitude, location.longitude))
print(type(location))
#print(location[0].raw['address_components'][-1]["long_name"])