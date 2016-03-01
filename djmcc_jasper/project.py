# modules
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

# source files
import reset
import get
import clean
import kmeans
import merge
import zoning

exec(open('../pymongo_dm.py').read())

# connect to DBMS

print("Connecting to the DBMS...")

client = pymongo.MongoClient()
repo   = client.repo
repo.authenticate('djmcc_jasper', 'djmcc_jasper')

# execute scripts

reset.run(repo)
get.run(repo)
clean.run(repo)
merge.run(repo)
kmeans.run(repo)
zoning.run(repo)

# disconnect from the DBMS

print("Disconnecting from the DBMS...")

repo.logout()

# EOF