"""
Calculates p score for each stop - based on how important we think the
	stop is and if it is handicap accessible. 

Relies on normal_ppl_sec_util, t_stop_locations datasets
"""

import pymongo
import prov.model
import time
import datetime
import uuid
import urllib.request
import json
import scipy.stats


exec(open('../pymongo_dm.py').read())


from math import sqrt

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

def permute(k,x):
	shuffled = x[k:] + x[:k]
	return shuffled

def avg(x): # Average
	return sum(x)/len(x)

def stddev(x): # Standard deviation.
	m = avg(x)
	return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
	return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
	if stddev(x)*stddev(y) != 0:
		return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
	c0 = corr(x, y)
	print(c0)
	corrs = []
	for k in range(0, len(y)):
		y_permuted = permute(k,y)
		corrs.append(corr(x, y_permuted))
	print(corrs)
	return len([c for c in corrs if abs(c) > c0])/len(corrs)

def changeWheelchairValue(l):
	return [(n,i,1) if (b == '1') else (n,i,0) for (n,i,b) in l]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def main():
	out_coll = 'p_score_stops'
	repo.dropPermanent(out_coll)
	repo.createPermanent(out_coll)
	startTime = datetime.datetime.now()

	# Get all utility measurements.
	sws = repo['{}.{}'.format(teamname, 'normal_ppl_sec_util')].find({})
	ss = repo['{}.{}'.format(teamname, 't_stop_locations')].find({})

	# Get some tuples from the cursors to start with.
	stop_weights = [(s['stop'], s['ppl-secs'], s['line']) for s in sws]
	# Remove duplicate points, for those on multiple branches.
	stops = set([(s['stop_name'], s['stop_id'], s['wheelchair_boarding']) for s in ss])
	

	# 'wheelchair_boarding' = 0 means the information is unavailable
	# 'wheelchair_boarding' = 2 means the stop is not wheelchair accessible
	# change 'wheelchair_boarding' values from 2 to 0 (to make it easier for p-value calc)
	updated_stops = changeWheelchairValue(stops)


	# combines updated_stops and stops sets if 'stop' and 'stop_id' are the same
	joined_stops = [(i, (n, int(w), int(ppl), l)) for n, i, w in updated_stops for s, ppl, l in stop_weights if s == i]

	
	
	sums = aggregate([((i,w),ppl) for (i,(n,w,ppl,l)) in joined_stops], sum)
	counts = aggregate([(i,1) for (i,(n,w,ppl,l)) in joined_stops], sum)
	means = ([(w,b/d) for ((x,w),b) in sums for y,d in counts if x == y])


	(coefficient, pValue) = scipy.stats.pearsonr([w for w,ppl in means], [ppl for w,ppl in means])
	print(pValue)

	repo['{}.{}'.format(teamname, out_coll)].insert_one({'coefficient':coefficient, 'pValue':pValue})


	endTime = datetime.datetime.now()

	# Record the provenance document.
	#doc = create_prov(startTime, endTime)
	#repo.record(doc.serialize())
	#print(doc.get_provn())

	repo.logout()


if __name__ == '__main__':
	main()



