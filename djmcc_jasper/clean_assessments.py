##########################################################
# Title:   get_assessments.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Filter and aggregate assessment data  
##########################################################

import statistics
import dml
import prov.model
import uuid
import datetime

# mapping of ptypes to number of units for boundry checking

def checkNum(ptype, numUnits):
    if ptype == '111':
        if numUnits < 4:
            return 4
        elif numUnits <= 6:
            return numUnits
        else:
            return 6
    elif ptype == '112':
        if numUnits < 7:
            return 7
        elif numUnits <= 30:
            return numUnits
        else:
            return 30
    elif ptype == '113':
        if numUnits < 31:
            return 31
        elif numUnits <= 99:
            return numUnits
        else:
            return 99
    elif ptype == '114':
        if numUnits < 100:
            return 100
        else:
            return numUnits
    else: # ptype == 109 or 115
        return numUnits

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    # check for existance of important fields

    print('Removing incomplete records...')

    result = repo.djmcc_jasper.assessments.delete_many({'$or': [
                        {'ptype':       {'$exists': False}},
                        {'pid':         {'$exists': False}},
                        {'st_num':      {'$exists': False}},
                        {'st_name':     {'$exists': False}},
                        {'st_name_suf': {'$exists': False}},
                        {'zipcode':     {'$exists': False}},
                        {'av_land':     {'$exists': False}},
                        {'av_bldg':     {'$exists': False}},
                        {'av_total':    {'$exists': False}},
                        {'living_area': {'$exists': False}},
                        {'zipcode':     '02186'} ]})

    print('Removed ' + str(result.deleted_count) + ' incomplete records...')

    # filter out non-residential properties and any building assessment < $50,000

    print('Removing non-applicable records and building filtered assessments...')

    for asmt in repo.djmcc_jasper.assessments.find():
        ptype = int(asmt['ptype'])
        av_bldg = int(asmt['av_bldg'])
        living_area = int(asmt['living_area'])
        num = asmt['st_num']
        if ptype > 100 and \
           ptype < 116 and \
           av_bldg >= 50000 and \
           living_area > 0:
            repo.djmcc_jasper.filtered_assessments.insert_one(asmt)
            if asmt['st_name_suf'] == 'AVE':
                repo.djmcc_jasper.filtered_assessments.update_one({'_id': asmt['_id']}, {'$set': {'st_name_suf': 'AV'}})

    print('Removed ' + str(repo.djmcc_jasper.assessments.count() - repo.djmcc_jasper.filtered_assessments.count()) + ' non-applicable records...')

    # aggregate multiple units at the same address (e.g. condos @ 520 Beacon St 02215)
    # return the median address to avoid skew due to outlier data

    print('Adding the address field to all records...')

    for asmt in repo.djmcc_jasper.filtered_assessments.find():
        address = ''
        if not asmt['st_name_suf'] == ' ':
            address = asmt['st_num'] + ' ' + asmt['st_name'] + \
                      ' ' + asmt['st_name_suf'] + ' ' + asmt['zipcode']
        else:
            address = asmt['st_num'] + ' ' + asmt['st_name'] + ' ' + asmt['zipcode']
        repo.djmcc_jasper.filtered_assessments.update_one({'_id': asmt['_id']}, {'$set': {'address': address}})

    # commented because we are currently unable to index using the authenticated repo:
    # repo.djmcc_jasper.filtered_assessments.create_index('address')

    print('Computing value per unit and collapsing records with the same address into a single entry...')

    seenAddresses = [] # CAN YOU MAKE THIS A SET LIKE YOU DID IN JOIN!? HASH >> LIST
    mergedAsmts = []
    nonAlphaNumCount = 0

    cursor = repo.djmcc_jasper.filtered_assessments.find({}, no_cursor_timeout=True)

    for asmt in cursor:
        if asmt['address'] not in seenAddresses: # this statement asserts each address appears at most once
            seenAddresses.append(asmt['address'])
            repo.djmcc_jasper.merged_assessments.insert_one(asmt)
            matches = [match for match in repo.djmcc_jasper.filtered_assessments.find({'address': asmt['address']})]
            if len(matches) > 1:
                values = []
                for match in matches:
                    values.append(int(match['av_total']))
                valueMedian = int(statistics.median(values))
                valueRange = max(values) - min(values)
                repo.djmcc_jasper.merged_assessments.update_one({'_id': asmt['_id']}, 
                                                                {'$set': {'av_per_unit': valueMedian, 
                                                                          'av_range':    valueRange,
                                                                          'num_units':   str(len(matches))} })
            else: # single match - calculate per unit assessment
                ptype = asmt['ptype']
                value = ''
                numUnits = 0
                living_area = int(asmt['living_area'])
                if ptype == '101':
                    numUnits = 1
                elif ptype == '104':
                    numUnits = 2
                elif ptype == '105':
                    numUnits = 3
                else:
                    numUnits = 1 + living_area // 1000
                    numUnits = checkNum(ptype, numUnits)
                value = int(asmt['av_total']) // numUnits
                repo.djmcc_jasper.merged_assessments.update_one({'_id': asmt['_id']},
                                                                {'$set': {'av_per_unit': value, 
                                                                          'num_units':   str(numUnits)} })

        if not asmt['st_num'].isalnum(): # chose not to handle street num ranges directly
            nonAlphaNumCount += 1  

    cursor.close()

    print('Collapsed ' + str(repo.djmcc_jasper.filtered_assessments.count() - repo.djmcc_jasper.merged_assessments.count()) + ' records...')
    print('There are ' + str(nonAlphaNumCount) + ' records with a non-alpha numeric street number...')

    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')

    # agent: script
    agt_script = doc.agent('alg:clean_assessments', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data set
    dat_assessments = doc.entity('dat:assessments', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})

    # activity: compute
    act_clean = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    # entity: data set
    dat_filtered_assessments = doc.entity('dat:filtered_assessments', {prov.model.PROV_LABEL:'Filtered Assessments', prov.model.PROV_TYPE:'ont:DataSet'})

    # edges
    doc.wasAssociatedWith(act_clean, agt_script)
    doc.used(act_clean, dat_assessments, startTime)
    doc.used(act_clean, dat_filtered_assessments, startTime)

    # edges
    doc.wasAttributedTo(dat_filtered_assessments, agt_script)
    doc.wasGeneratedBy(dat_filtered_assessments, act_clean, endTime)
    doc.wasDerivedFrom(dat_filtered_assessments, dat_assessments, act_clean, act_clean, act_clean)

    # ent: data set
    dat_merged_assessments = doc.entity('dat:merged_assessments', {prov.model.PROV_LABEL:'Merged Assessments', prov.model.PROV_TYPE:'ont:DataSet'})
    
    # edges
    doc.wasAttributedTo(dat_merged_assessments, agt_script)
    doc.wasGeneratedBy(dat_merged_assessments, act_clean, endTime)
    doc.wasDerivedFrom(dat_merged_assessments, dat_filtered_assessments, act_clean, act_clean, act_clean)    

    repo.record(doc.serialize()) 

#EOF
