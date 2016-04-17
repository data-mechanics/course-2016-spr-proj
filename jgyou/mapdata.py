'''
script to display map of data points

Acknowledgments:
Thanks to Ben Lawson for suggesting the use of Folium for mapping.

Also used Maptime Boston resources to create initial map.
http://maptimeboston.github.io/leaflet-intro/

'''

import folium
import urllib.request

f, headers = urllib.request.urlretrieve('http://maptimeboston.github.io/leaflet-intro/neighborhoods.geojson')

f2 = urllib.request.urlopen("http://datamechanics.io/data/jgyou/seniorservices.json").read().decode("utf-8")



map_boston = folium.Map(location=[42.35, -71.08], zoom_start=13, tiles='Stamen Toner')

folium.GeoJson(open(f), name='geojson').add_to(map_boston)



map_boston.save('map.html')