'''
File:	extract_zip_info.py
Date:	02/24/16

This file is to extract zip and corresponding longtitudes and 
latitudes information from approved_building_permits dataset 
in mongodb.
'''

from get_auth import auth
from get_repo import get_mongodb_repo
from set_operation import project

# Set up the database connection.
repo = get_mongodb_repo(auth['admin']['name'], auth['admin']['pwd'])
db = repo[auth['admin']['name']+'.'+'approved_building_permits']

cursor = db.find()
for document in cursor:
	print(document)