import urllib.request
import json
import re

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

repo.createPermanent("plan")