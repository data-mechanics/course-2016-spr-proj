import pymongo
import datetime
import prov.model
import uuid

def run(repo):

    startTime = datetime.datetime.now()

    # Future work - Allow indexing for faster processing.

    '''
    # Create index on zipcode for each collection:
    print("Creating zip code index on 2014 Property Assessments...")
    repo.djmcc_jasper.assessments_2014.create_index([('zipcode', pymongo.ASCENDING)])
    
    print("Creating zip code index on 2015 Property Assessments...")
    repo.djmcc_jasper.assessments_2015.create_index([('zipcode', pymongo.ASCENDING)])
    
    print("Creating zip code index on Approved Permits...")
    repo.djmcc_jasper.approved_permits.create_index([('zip',     pymongo.ASCENDING)])
    '''

    # Delete all records missing a critcal field.
    
    print("Cleaning 2014 Property Assessments...")
    
    result = repo.djmcc_jasper.assessments_2014.delete_many({'$or': [
                        {"parcel_id": {'$exists': False}}, 
                        {"zipcode":   {'$exists': False}},
                        {"ptype":     {'$exists': False}},
                        {"av_land":   {'$exists': False}},
                        {"av_bldg":   {'$exists': False}},
                        {"av_total":  {'$exists': False}},
                        {"gross_tax": {'$exists': False}}, 
                        {"zipcode": "NULL"} ]})

    print("Removed " + str(result.deleted_count) + " records from 2014 Property Assessments...")

    print("Cleaning 2015 Property Assessments...")
    
    result = repo.djmcc_jasper.assessments_2015.delete_many({'$or': [
                        {"pid":       {'$exists': False}}, 
                        {"zipcode":   {'$exists': False}},
                        {"ptype":     {'$exists': False}},
                        {"av_land":   {'$exists': False}},
                        {"av_bldg":   {'$exists': False}},
                        {"av_total":  {'$exists': False}},
                        {"gross_tax": {'$exists': False}},
                        {"zipcode": "02186"} ]})

    print("Removed " + str(result.deleted_count) + " records from 2015 Property Assessments...")

    print("Cleaning Approved Permits...")
    
    result = repo.djmcc_jasper.approved_permits.delete_many({'$or': [
                        {"parcel_id":          {'$exists': False}}, 
                        {"zip":                {'$exists': False}},
                        {"occupancytype":      {'$exists': False}},
                        {"declared_valuation": {'$exists': False}},
                        {"location":           {'$exists': False}},
                        {"issued_date":        {'$exists': False}},
                        {"issued_date":        {'$lt': '2014-01-01T00:00:00'}},
                        {"issued_date":        {'$gt': '2016-01-01T00:00:00'}}, 
                        {"zip": "02315"},
                        {"zip": "02026"} ]})

    print("Removed " + str(result.deleted_count) + " records from Approved Permits...")

    endTime = datetime.datetime.now()

    # Create the provenance document describing everything happening
    # in this script. Each run of the script will generate a new
    # document describing that invocation event. This information
    # can then be used on subsequent runs to determine dependencies
    # and "replay" everything. The old documents will also act as a
    # log.

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    
    # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.

    this_script = doc.agent('alg:clean', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    dataset_2014    = doc.entity('dat:assessments_2014', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    dataset_2015    = doc.entity('dat:assessments_2015', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    dataset_permits = doc.entity('dat:approved_permits', {'prov:label':'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

    clean_2014    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
    clean_2015    = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
    clean_permits = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    doc.wasAssociatedWith(clean_2014, this_script)
    doc.wasAssociatedWith(clean_2015, this_script)
    doc.wasAssociatedWith(clean_permits, this_script)

    # used(activity, entity, time)

    doc.used(clean_2014, dataset_2014, startTime)
    doc.used(clean_2015, dataset_2015, startTime)
    doc.used(clean_permits, dataset_permits, startTime)

    # stored datasets

    prov_assessments_2014 = doc.entity('dat:assessments_2014', {prov.model.PROV_LABEL:'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_assessments_2014, this_script)
    doc.wasGeneratedBy(prov_assessments_2014, clean_2014, endTime)
    doc.wasDerivedFrom(prov_assessments_2014, dataset_2014, clean_2014, clean_2014, clean_2014)

    prov_assessments_2015 = doc.entity('dat:assessments_2015', {prov.model.PROV_LABEL:'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_assessments_2015, this_script)
    doc.wasGeneratedBy(prov_assessments_2015, clean_2015, endTime)
    doc.wasDerivedFrom(prov_assessments_2015, dataset_2015, clean_2015, clean_2015, clean_2015)

    prov_permits = doc.entity('dat:approved_permits', {prov.model.PROV_LABEL:'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_permits, this_script)
    doc.wasGeneratedBy(prov_permits, clean_permits, endTime)
    doc.wasDerivedFrom(prov_permits, dataset_permits, clean_permits, clean_permits, clean_permits)

    repo.record(doc.serialize()) # Record the provenance document.
    # print(json.dumps(json.loads(doc.serialize()), indent=4))
    # print(doc.get_provn())

#EOF