import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import os
import sys

auth = open(sys.argv[1], 'r')

cred = json.load(auth)

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

os.system("py -3 retrieval.py")
print('done')
os.system("py -3 rodent_problems.py")
print('done')
os.system("py -3 inspections.py")
print('done')
os.system("py -3 get_closest_stop.py")
print('done')
os.system("py -3 get_crime.py")
print('done')
os.system("py -3 get_liquor.py")
print('done')
os.system("py -3 correlation.py")
print('done')
os.system("py -3 ratings.py")
print('done')


repo.logout()
