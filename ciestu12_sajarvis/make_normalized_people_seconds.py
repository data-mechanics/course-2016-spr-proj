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
    #doc = create_prov(startTime, endTime)
    #repo.record(doc.serialize())
    #print(doc.get_provn())

    repo.logout()


if __name__ == '__main__':
    main()
