"""
Use the stop boarding counts and nearest stop datasets to come up with a
measure of utility provided by each stop, called 'people seconds'. This
measure can be used to determine the value of each stop.

Requires the collections:
    'nearest_stops'
    'green_line_boarding_counts'

Lower scores indicate a higher value, considering the overall time saved for
commuters.
"""
import json
import pymongo
import prov.model
import time
import datetime
import uuid

# Estimated time of the time cost (seconds) for stopping at a T stop.
STOP_TIME = 60

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]

def main():
    teamname = 'ciestu12_sajarvis'
    # Set up the database connection.
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(teamname, teamname)
    out_coll = 'people_second_utility'
    repo.dropPermanent(out_coll)
    repo.createPermanent(out_coll)

    startTime = datetime.datetime.now()

    # get DB for population and nearest stop
    stop_pop = repo['{}.{}'.format(teamname, 'green_line_boarding_counts')].find({})
    nearest_stops = repo['{}.{}'.format(teamname, 'nearest_stops')].find({})

    # project into some tuples the information we need
    nearest = [(s['stop'], s['line'], s['nearest'], s['time_sec']) for s in nearest_stops]
    pop = [(s['stop_id'], s['stop_boardings']) for s in stop_pop]

    dot = product(nearest, pop)
    matches = [(f,g) for (f,g) in dot if f[0] == g[0]]

    # aggregate to find the total usage on each branch
    line_usages = aggregate([(l,pop) for ((s,l,n,w),(i,pop)) in matches], sum)
    for line,total_usage in line_usages:
        # Measure the utility of each stop on each branch
        # ultimately join nearest and pop on the branch to keep it on the
        # same line.
        for stop,pop,sec in [(s,p,w) for ((s,l,n,w),(i,p)) in matches if l == line]:
            everyone_else = total_usage - pop
            # the actual measure of utility. low scores are best.
            ppl_seconds = (everyone_else * STOP_TIME) - (pop * sec)
            # now insert the people second utility into our data set.
            elements = {'stop':stop, 'ppl-secs':ppl_seconds, 'line':line}
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
    this_script = doc.agent('alg:make_people_seconds',
                            {
                                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],
                                'ont:Extension':'py'})
    nearest_resource = doc.entity('dat:nearest_stops',
                                  {
                                      'prov:label':'Nearest Neighboring Stops on Green Line',
                                      prov.model.PROV_TYPE:'ont:DataSet'})
    boarding_resource = doc.entity('dat:green_line_boarding_counts',
                                   {
                                       'prov:label':'Boarding Counts for Green Line Stops',
                                       prov.model.PROV_TYPE:'ont:DataSet'})
    this_run = doc.activity('log:a'+str(uuid.uuid4()),
                            startTime, endTime,
                            { prov.model.PROV_LABEL:'Compute Utility with Nearest Alternatives and Popularity',
                              prov.model.PROV_TYPE:'ont:Computation' })
    doc.wasAssociatedWith(this_run, this_script)
    doc.usage(this_run, nearest_resource, startTime, None,
            { prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'db.ciestu12_sajarvis.nearest_stops.find({})'})
    doc.usage(this_run, boarding_resource, startTime, None,
            { prov.model.PROV_TYPE:'ont:Retrieval',
              'ont:Query':'db.ciestu12_sajarvis.green_line_boarding_counts.find({})'})

    # Now define entity for the dataset we obtained.
    ppl_seconds = doc.entity('dat:people_second_utility',
                          {
                              prov.model.PROV_LABEL:'Measure of Utility Per Stop for Collective Riders',
                              prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(ppl_seconds, this_script)
    doc.wasGeneratedBy(ppl_seconds, this_run, endTime)
    doc.wasDerivedFrom(ppl_seconds, nearest_resource, this_run, this_run, this_run)
    doc.wasDerivedFrom(ppl_seconds, boarding_resource, this_run, this_run, this_run)

    return doc


if __name__ == '__main__':
    main()
