import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import os
import sys

exec(open('../pymongo_dm.py').read())

auth = open(sys.argv[1], 'r')

cred = json.load(auth)

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(cred['username'], cred['pwd'])

os.system("py -3 retrieval.py")
os.system("py -3 rodent_problems.py")
os.system("py -3 inspections.py")

repo.logout()