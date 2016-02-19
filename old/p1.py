# ####################################################################
#
# CS 591 - Data Mechanics (Prof. Lapets)
#
# p1.py
# by Daren McCulley (djmcc) and Jasper Burns (jasper)
#

import urllib.request
import json
import pymongo
# import prov.model
import datetime
import uuid

# ############################### SETUP ################################

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('djmcc_jasper', 'djmcc_jasper')

startTime = datetime.datetime.now()

# ############################## GET DATA ###############################

# Retrieve classification
url = './property_classification.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
repo.dropPermanent("property_classification")
repo.createPermanent("property_classification")
repo['djmcc_jasper.property_classification'].insert_many(r)

# ############################### SHUTDOWN ################################

endTime = datetime.datetime.now()
repo.logout()

# eof