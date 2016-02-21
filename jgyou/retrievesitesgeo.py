from urllib import parse, request
from geojson import dumps

param = "75 Amory Street, Boston​, MA 02130"
key = ""
query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(param) + "&limit=1" + "&pretty=1" + "&countrycode=us" +"&key=" + key

georesult =  request.urlopen(query).read().decode("utf-8")

georesult2 = dumps(georesult, sort_keys=True)

print(georesult2)




