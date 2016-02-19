# modules
import urllib.request
import json
import pymongo
#import prov.model
import datetime
import uuid
# other files
import get
import clean
import count
import collapse_ptype_desc


# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('djmcc_jasper', 'djmcc_jasper')

# For provenance
startTime = datetime.datetime.now()

# Import data
get.run(repo)

# Create collection abstractions.
assessments_2014 = repo.djmcc_jasper.assessment_2014
assessments_2015 = repo.djmcc_jasper.assessment_2015
approved_permits = repo.djmcc_jasper.approved_permits

# Clean data
clean.run(assessments_2014,assessments_2015,approved_permits)

# Analyze data
count.run(assessments_2014,assessments_2015,approved_permits)
collapse_ptype_desc.run(repo, assessments_2014,assessments_2015,approved_permits)

# For provenance
endTime = datetime.datetime.now()

repo.logout()

# eof
