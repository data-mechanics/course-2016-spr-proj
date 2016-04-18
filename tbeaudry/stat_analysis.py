import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid
import time

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('tbeaudry', 'tbeaudry')


