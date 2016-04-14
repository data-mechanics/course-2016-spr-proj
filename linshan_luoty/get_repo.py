import pymongo

exec(open('get_auth.py').read())

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(auth['admin']['name'], auth['admin']['pwd'])