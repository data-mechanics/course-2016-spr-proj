import urllib.request
import json

url = "https://data.cityofboston.gov/resource/ufcx-3fdn.json?$limit=10"
response = urllib.request.urlopen(url).read().decode("utf-8")
print(json.dumps(json.loads(response), sort_keys=True, indent=2))
