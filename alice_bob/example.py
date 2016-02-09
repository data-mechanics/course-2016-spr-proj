import urllib.request
import json
import pymongo

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('alice_bob', 'alice_bob')
repo.dropPermanent("found")
repo.createPermanent("found")
repo['alice_bob.found'].insert_many(r)
repo.logout()

## eof