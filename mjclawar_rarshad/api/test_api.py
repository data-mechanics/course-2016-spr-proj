"""
Testing API from data.cityofboston.gov

Michael Clawar and Raaid Arshad
"""

import urllib.request
import json
import pymongo

exec(open('../../pymongo_dm.py').read())

url = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json?$limit=10'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
print(s)

# client = pymongo.MongoClient()
# repo = client.repo
# repo.authenticate('mjclawar_rarshad', 'mjclawar_rarshad')
# repo.dropPermanent("found")
# repo.createPermanent("found")
# repo['mjclawar_rarshad.found'].insert_many(r)
# repo.logout()
