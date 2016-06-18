##########################################################
# Title:   collapse_apartments.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Aggregate by (lat, lng) and avg rent 
##########################################################

import dml
import statistics
import prov.model
import uuid
import datetime

# for a given (lat, lng) return the mean rent

def findMean(repo, location):
    (lat, lng) = location
    rents = []
    for apt in repo.djmcc_jasper.apartments.find({'lat': lat, 'lng': lng}):
        rents.append(apt['rent'])
    return int(statistics.mean(rents))

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    # commented because we are currently unable to index using the authenticated repo:
    # repo.djmcc_jasper.apartments.create_index([('lat', pymongo.ASCENDING), ('lng', pymongo.ASCENDING)])
    
    print('Collapsing records with the same location into a single entry...')

    locationsSeen = set()

    for apt in repo.djmcc_jasper.apartments.find():
        location = (apt['lat'], apt['lng'])
        if location not in locationsSeen:
            locationsSeen.add(location)
            meanRent = findMean(repo, location)
            repo.djmcc_jasper.collapsed_apartments.insert_one(apt)
            repo.djmcc_jasper.collapsed_apartments.update_one({'_id': apt['_id']}, 
                                                              {'$set': {'rent': meanRent}})

    print('Collapsed %s records into %s records with a unique location...' \
          % (repo.djmcc_jasper.apartments.count(), repo.djmcc_jasper.collapsed_apartments.count()))
    
    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    # agent: script
    agt_script = doc.agent('alg:collapse_apartments', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data set
    dat_apartments = doc.entity('dat:apartments', {prov.model.PROV_LABEL:'Apartments', prov.model.PROV_TYPE:'ont:DataSet'})

    # activity: compute
    act_collapse = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    # edges
    doc.wasAssociatedWith(act_collapse, agt_script)
    doc.used(act_collapse, dat_apartments, startTime)

    # entity: data set
    dat_collapsed_apartments = doc.entity('dat:collapsed_apartments', {prov.model.PROV_LABEL:'Collapsed Apartments', prov.model.PROV_TYPE:'ont:DataSet'})
    
    # edges
    doc.wasAttributedTo(dat_collapsed_apartments, agt_script)
    doc.wasGeneratedBy(dat_collapsed_apartments, act_collapse, endTime)
    doc.wasDerivedFrom(dat_collapsed_apartments, dat_apartments, act_collapse, act_collapse, act_collapse)

    repo.record(doc.serialize())

#EOF
