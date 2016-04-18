from urllib import request, parse
import json
import pymongo
import prov.model
import datetime
import uuid
import time
import csv
import re

# retrieve certain elements from the Location Affordability Index by Block dataset

with open("lai_data_14460_blkgrps.csv") as f2:
	laiinfo = csv.reader(f2)
	laiarr = []
	headers = next(laiinfo, None)
	for row in laiinfo:
		censusblock = int(row[headers.index("blkgrp")].replace("'", ""))
		rai = float(row[headers.index("retail_access_index")])
		laiarr.append((censusblock, rai))
	f2.close()

	# then using score output, get addr, censusblock, etc.
	
	with open("scores.json") as f:
		scores = json.loads(f.read())
		output = [[None for j in range(5)] for i in range(len(scores) + 1)]
		for i, s in enumerate(scores):
			addr = s['address']
			censusblock = s['census_block']
			score = s['score']
			output[i + 1][0] = addr
			output[i + 1][1] = censusblock
			output[i + 1][3] = score

			if re.search("[0-9]{5,5}", addr):
				zipcode = re.findall("[0-9]{5,5}", addr)[0]
			else:
				zipcode = None

			output[i + 1][2] = zipcode

			result = [rai for (cb, rai) in laiarr if str(cb) in str(censusblock)]

			# if there is no matching census block info, take 
			if len(result) == 0:
				result2 = [rai for (cb, rai) in laiarr if str(cb)[0:len(str(cb))-1] in str(censusblock)]
				if len(result2) != 0:
					output[i + 1][4] = result2[0]
			else:
				output[i + 1][4] = result[0]

		f.close()

		output[0] = ["address", "censusblock", "zipcode", "score", "rai"]

		# write out this information to csv file to be used in d3 plots

		with open("plot.csv", "w") as f3:
			plt = csv.writer(f3)
			plt.writerows(output)
			f3.close()
			
		
	



