##########################################################
# Title:   join.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Join assessment and apartment data
#          on the address attribute 
##########################################################

import pymongo
import prov.model
import uuid
import datetime

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    print('Joining on address between merged_assessments and filtered_apartments...')

    seenAddresses = set()
    for apt in repo.djmcc_jasper.filtered_apartments.find():
        if apt['address'] not in seenAddresses:
            seenAddresses.add(apt['address'])
            for asmt in repo.djmcc_jasper.merged_assessments.find({'address': apt['address']}):
                repo.djmcc_jasper.joined.insert_one(apt)
                if 'av_range' in asmt.keys():
                    repo.djmcc_jasper.joined.update_one({'_id': apt['_id']},
                                         {'$set': {'av_per_unit': asmt['av_per_unit'],
                                                   'av_range':    asmt['av_range'],
                                                   'num_units':   asmt['num_units'],
                                                   'ptype':       asmt['ptype'],
                                                   'pid':         asmt['pid']} })
                else:
                    repo.djmcc_jasper.joined.update_one({'_id': apt['_id']},
                                         {'$set': {'av_per_unit': asmt['av_per_unit'],
                                                   'num_units':   asmt['num_units'],
                                                   'ptype':       asmt['ptype'],
                                                   'pid':         asmt['pid']} })

    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    # agent: script
    agt_script = doc.agent('alg:join', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data set
    dat_merged_assessments = doc.entity('dat:assessments', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})
    dat_filtered_apartments = doc.entity('dat:filtered_apartments', {prov.model.PROV_LABEL:'Filtered Apartments', prov.model.PROV_TYPE:'ont:DataSet'})

    # activity: compute
    act_join = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    # edges
    doc.wasAssociatedWith(act_join, agt_script)
    doc.used(act_join, dat_merged_assessments, startTime)
    doc.used(act_join, dat_filtered_apartments, startTime)

    # entity: data set
    dat_joined = doc.entity('dat:joined', {prov.model.PROV_LABEL:'Joined', prov.model.PROV_TYPE:'ont:DataSet'})
    
    # edges
    doc.wasAttributedTo(dat_joined, agt_script)
    doc.wasGeneratedBy(dat_joined, act_join, endTime)
    doc.wasDerivedFrom(dat_joined, dat_merged_assessments, act_join, act_join, act_join)
    doc.wasDerivedFrom(dat_joined, dat_filtered_apartments, act_join, act_join, act_join)

    repo.record(doc.serialize())

#EOF
