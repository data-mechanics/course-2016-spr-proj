###############################################
# Title:   project.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Driver script for Project 2 (CS 591)
###############################################

# modules
import urllib.request
import json
import pymongo
import time
import sys

# source files
import reset
import get_apartments
import collapse_apartments
import clean_apartments
import get_assessments
import clean_assessments
import join
import compute_ratio
import comp_vit

exec(open('../pymongo_dm.py').read())

print('Connecting to the DBMS...')
client = pymongo.MongoClient()
repo   = client.repo
repo.authenticate('djmcc_jasper', 'djmcc_jasper')

f = open(sys.argv[1]).read()
authFile = json.loads(f)
print("Received auth file" + str(authFile) + "...")

# run scripts
reset.run(repo)
get_apartments.run(repo)
collapse_apartments.run(repo)
clean_apartments.run(repo)
get_assessments.run(repo)
clean_assessments.run(repo)
join.run(repo)
compute_ratio.run(repo)
comp_vit.run(repo,authFile)

print('Disconnecting from the DBMS...')
repo.logout()

#EOF
