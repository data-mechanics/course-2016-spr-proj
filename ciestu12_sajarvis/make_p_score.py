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

# 'wheelchair_boarding' = 0 means the information is unavailable
# 'wheelchair_boarding' = 2 means the stop is not wheelchair accessible
# change 'wheelchair_boarding' values from 2 to 0 (to make it easier for p-value calc)
# for more info on these values, see:  https://developers.google.com/transit/gtfs/reference#stopstxt
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
	stop_weights = [(s['stop'], s['ppl_secs'], s['line']) for s in sws]
	# Remove duplicate points, for those on multiple branches.
	stops = set([(s['stop_name'], s['stop_id'], s['wheelchair_boarding']) for s in ss])

	updated_stops = changeWheelchairValue(stops)


	# combines updated_stops and stops sets if 'stop' and 'stop_id' are the same
	joined_stops = [(i, (n, int(w), int(ppl), l)) for n, i, w in updated_stops for s, ppl, l in stop_weights if s == i]

	sums = aggregate([((i,w),ppl) for (i,(n,w,ppl,l)) in joined_stops], sum)
	counts = aggregate([(i,1) for (i,(n,w,ppl,l)) in joined_stops], sum)
	means = ([(w,b/d) for ((x,w),b) in sums for y,d in counts if x == y])

	# use scipy to calcualte p-value and correlation coefficient
	(coefficient, pValue) = scipy.stats.pearsonr([w for w,ppl in means], [ppl for w,ppl in means])
	print(pValue)

	# store correlation coefficient and p-value
	repo['{}.{}'.format(teamname, out_coll)].insert_one({'coefficient':coefficient, 'pValue':pValue})


	endTime = datetime.datetime.now()

	# Record the provenance document.
	doc = create_prov(startTime, endTime)
	repo.record(doc.serialize())
	print(doc.get_provn())

	repo.logout()

def create_prov(startTime, endTime):
    '''Create the provenance document for file.'''
    # Create provenance data and recording
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ciestu12_sajarvis/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/ciestu12_sajarvis/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    # This run has an agent (the script), entities (the sources), and an activity (execution)
    this_script = doc.agent('alg:make_p_score',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    normalized_resource = doc.entity('dat:normal_ppl_sec_util',
                                     {
                                         'prov:label':'Normalized People Second Utility Rating',
                                         prov.model.PROV_TYPE:'ont:DataSet'})
    stop_location_resource = doc.entity('dat:t_stop_locations',
                                        {
                                            'prov:label':'Locations for all MBTA Stops',
                                            prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            { prov.model.PROV_LABEL:'Compute Correlation and P Value for Utility vs Wheelchair Access',
                              prov.model.PROV_TYPE:'ont:Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, normalized_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.normal_ppl_sec_util.find({})'})
    doc.usage(this_run, stop_location_resource, startTime, None,
              { prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'db.ciestu12_sajarvis.t_stop_locations.find({})'})

    # Now define entity for the dataset we obtained.
    p_score = doc.entity('dat:p_score_stops',
                               {
                                   prov.model.PROV_LABEL:'Correlation and P Value for Utility vs Wheelchair Access',
                                   prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(p_score, this_script)
    doc.wasGeneratedBy(p_score, this_run, endTime)
    doc.wasDerivedFrom(p_score, normalized_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(p_score, stop_location_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
	main()
