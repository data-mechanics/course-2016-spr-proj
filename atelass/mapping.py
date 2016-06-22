'''
IMPORTANT: Use Python 2 for this script, as gmplot doesn't seem to work in Python 3.
This script maps the crimes and streetlights around a given station.
'''

import sys
# Check Python version.
if sys.version_info.major == 3:
    sys.exit('This script must be run in Python 2 (you are attempting to run it in Python 3)')

import pymongo
import time
import gmplot

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('atelass', 'atelass')

# Get latitudes and longitudes of crimes.
print(time.strftime('%I:%M:%S%p') + ': Getting locations of all crimes chosen via previous scripts.')
crime_latitudes = []
crime_longitudes = []
for doc in repo['atelass.crimes_and_streetlights'].find():
    crime_compnos = doc['crime_compnos']
    crime = repo['atelass.crimes'].find({'compnos': crime_compnos})[0]
    crime_latitudes.append(float(crime['location']['latitude']))
    crime_longitudes.append(float(crime['location']['longitude']))
print(time.strftime('%I:%M:%S%p') + ': Completed.')

print(time.strftime('%I:%M:%S%p') + ': Getting locations of all streetlights near crimes chosen via previous scripts.')
streetlight_locations = []
for doc in repo['atelass.crimes_and_streetlights'].find():
    for streetlight_location in doc['streetlight_locations']:
        streetlight_locations.append(streetlight_location)
streetlight_latitudes = [xi for (xi, yi) in streetlight_locations]
streetlight_longitudes = [yi for (xi, yi) in streetlight_locations]
print(time.strftime('%I:%M:%S%p') + ': Completed.')

# Plot points on map, save to an html file called 'crimes_and_streetlights.html'.
print(time.strftime('%I:%M:%S%p') + ': Plotting points on map.')
gmap = gmplot.GoogleMapPlotter(42.358056, -71.063611, 14)
gmap.scatter(crime_latitudes, crime_longitudes, 'red')
gmap.scatter(streetlight_latitudes, streetlight_longitudes, 'yellow')
gmap.draw('crimes_and_streetlights.html')
print(time.strftime('%I:%M:%S%p') + ': Completed.')
print
print('On the map, crimes are red and streetlights are yellow.')
