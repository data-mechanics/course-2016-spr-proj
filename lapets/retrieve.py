import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import zipfile
import io

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('lapets', 'lapets')

# Record the starting time.
startTime = datetime.datetime.now()

# Create the destination collection.
repo.dropPermanent("osmboston")
repo.createPermanent("osmboston")

# Retrieve the metro extract archive.
url = 'https://s3.amazonaws.com/metro-extracts.mapzen.com/boston_massachusetts.osm2pgsql-geojson.zip'
response = urllib.request.urlopen(url).read()
# If we want to write the zip file to disk.
# open('boston_massachusetts.osm2pgsql-geojson.zip', 'wb').write(response) 

z = zipfile.ZipFile(io.BytesIO(response))
for file in z.namelist():
    if file in ['boston_massachusetts_osm_line.geojson', 'boston_massachusetts_osm_point.geojson']:
        geojson = json.loads(z.open(file).read().decode("latin-1"))
        repo['lapets.osmboston'].insert_many(geojson['features'])
        # If we want to write the file to disk.
        # open(name, 'wb').write(z.open(file).read())

# Close the database connection.
repo.logout()

# Record the ending time.
endTime = datetime.datetime.now()

## eof
