##########################################################
# Title:   join.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Computes the assessment : rent ratio.
#          Additionally this script generates data.json.
##########################################################

import pymongo
import statistics
import prov.model
import uuid
import datetime

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()
    
    print('Computing rent-assessment ratio and adding it to each record...')

    # ratio is used as a filter for removing outliers from the visualization dataset

    for record in repo.djmcc_jasper.joined.find():
        ratio = record['av_per_unit'] / record['rent']
        repo.djmcc_jasper.joined.update_one({'_id': record['_id']}, {'$set': {'ratio': ratio}})

    # per zipcode output

    for zipcode in repo.djmcc_jasper.joined.distinct('zipcode'):
        ratios = []
        for record in repo.djmcc_jasper.joined.find({'zipcode': zipcode}):
            ratios.append(record['ratio'])
        print('For %s rentals in %s the average ratio is: %s...' % \
              (repo.djmcc_jasper.joined.find({'zipcode': zipcode}).count(), zipcode, statistics.mean(ratios)))

    # output data for visualization

    with open('./part1/assessment_rent_data.json', 'w') as f:

        # filter outliers or unwanted data

        print('Generating data.json for Part 1...')

        cursor = repo.djmcc_jasper.joined.find({'$and': [
                            {'av_per_unit': {'$lt': 1000000}},
                            {'av_per_unit': {'$gt': 100000}},
                            {'ratio':       {'$lt': 500}},
                            {'ratio':       {'$gt': 20}} ]})

        print('Removed %s outlier records...' % 
            str(repo.djmcc_jasper.joined.count() - cursor.count()))

        f.write('[\n')

        for record in cursor:
            if cursor.alive:
                f.write('\t{"rent": ' + str(record['rent']) + ',' + \
                        '"av_per_unit": ' + str(record['av_per_unit']) + ',' + \
                        '"address": "' + str(record['address']) + '",' + \
                        '"id": "' + str(record['id']) + '",' + \
                        '"pid": "' + str(record['pid']) + '",' + \
                        '"ptype": "' + str(record['ptype']) + '",' + \
                        '"zipcode": "' + str(record['zipcode']) + '"},\n')
            else:
                f.write('\t{"rent": ' + str(record['rent']) + ',' + \
                        '"av_per_unit": ' + str(record['av_per_unit']) + ',' + \
                        '"address": "' + str(record['address']) + '",' + \
                        '"id": "' + str(record['id']) + '",' + \
                        '"pid": "' + str(record['pid']) + '",' + \
                        '"ptype": "' + str(record['ptype']) + '",' + \
                        '"zipcode": "' + str(record['zipcode']) + '"}\n]')

    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    # agent: script
    agt_script = doc.agent('alg:compute_ratio', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data set
    dat_joined = doc.entity('dat:joined', {prov.model.PROV_LABEL:'Joined', prov.model.PROV_TYPE:'ont:DataSet'})

    # activity: compute
    act_compute_ratio = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    # edges
    doc.wasAssociatedWith(act_compute_ratio, agt_script)
    doc.used(act_compute_ratio, dat_joined, startTime)

    # entity: data set
    dat_joined = doc.entity('dat:joined', {prov.model.PROV_LABEL:'Joined', prov.model.PROV_TYPE:'ont:DataSet'})
    
    # edges
    doc.wasAttributedTo(dat_joined, agt_script)
    doc.wasGeneratedBy(dat_joined, act_compute_ratio, endTime)
    doc.wasDerivedFrom(dat_joined, dat_joined, act_compute_ratio, act_compute_ratio, act_compute_ratio)

    repo.record(doc.serialize())

#EOF
