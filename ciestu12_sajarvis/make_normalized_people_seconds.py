"""
We have a utility rating for each stop, but it's not directly usable as
a source for weights in k-means.
The ratings need to be normalized to be used as a weight.

Relies on the 'people_second_utility' dataset.
"""

import json
import pymongo
import prov.model
import time
import datetime
import uuid
from math import ceil

teamname = 'ciestu12_sajarvis'
# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate(teamname, teamname)

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]

def main():
    out_coll = 'normal_ppl_sec_util'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    startTime = datetime.datetime.now()

    # Get existing people second db.
    ppl_seconds = repo['{}.{}'.format(teamname, 'people_second_utility')].find({})

    # This adjustment is a bit subjective. We want a values to use as weights
    # for k-means and they currently have a large range, negative and positive,
    # with low indicated a higher score.
    # We'd like to bring them all positive, and within a reasonable range (tens
    # or hundreds instead of thousands or millions) to use as weights.
    pplsecs = [(s['ppl-secs'], s['stop'], s['line']) for s in ppl_seconds]

    print("Starting range is {} -> {}".format(
        min(pplsecs, key = lambda x: x[0]),
        max(pplsecs, key = lambda x: x[0])))

    # First invert, to make more desirable scores higher and heavier weights.
    inv_pplsecs = [(-p,(s,l)) for p,s,l in pplsecs]

    # Find the worst, so we can shift all up to at least positive.
    all_scores = [(1, p) for p,k in inv_pplsecs]
    worst_rating = abs([v for k,v in aggregate(all_scores, min)][0])
    shifted_scores = [(p + worst_rating + 1, k) for p,k in inv_pplsecs]

    # Now let's scale to a max
    scale = 1000
    all_scores = [(1, p) for p,k in shifted_scores]
    best_rating = [v for k,v in aggregate(all_scores, max)][0]
    ratio = best_rating / scale
    normalized_scores = [(ceil(p / ratio), s, l) for (p,(s,l)) in shifted_scores]

    print("Resulting range is {} -> {}".format(
        min(normalized_scores, key = lambda x: x[0]),
        max(normalized_scores, key = lambda x: x[0])))

    for p,s,l in normalized_scores:
        elements = {'ppl_secs':p, 'stop':s, 'line':l}
        print(elements)
        repo['{}.{}'.format(teamname, out_coll)].insert_one(elements)

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
    this_script = doc.agent('alg:make_normalized_people_seconds',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    pplsec_resource = doc.entity('dat:people_second_utility',
                                 {
                                     'prov:label':'Measure of Utility Per Stop for Collective Riders',
                                     prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            { prov.model.PROV_LABEL:'Compute Normalized Utility Based',
                              prov.model.PROV_TYPE:'ont:Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, pplsec_resource, startTime, None,
            { prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'db.ciestu12_sajarvis.people_second_utility.find({})'})

    # Now define entity for the dataset we obtained.
    normal_ppl_seconds = doc.entity('dat:normal_ppl_sec_util',
                                    {
                                        prov.model.PROV_LABEL:'Normalized People Second Utility Rating',
                                        prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(normal_ppl_seconds, this_script)
    doc.wasGeneratedBy(normal_ppl_seconds, this_run, endTime)
    doc.wasDerivedFrom(normal_ppl_seconds, pplsec_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
