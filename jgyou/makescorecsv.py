from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time
import csv



with open("scores.json") as f:
	scores = json.loads(f.read())
	output = [[None for j in range(len(5))] for i in range(len(20))]
	for s in scores:
		addr = s['address']
		censusblock = s['censusblock']
		score = s['score']



