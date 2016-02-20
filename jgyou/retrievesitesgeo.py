from urllib import parse, request
import json

param = ""
key = ""
query = "https://api.opencagedata.com/geocode/v1/geojson?q=" + parse.quote_plus(param) + "&key=" + key