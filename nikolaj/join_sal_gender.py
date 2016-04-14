import subprocess
import utils
import prov.model
import uuid

@utils.timestamped
def run():
    subprocess.call(['mongo repo -u nikolaj -p nikolaj --authenticationDatabase "repo" < join_sal_gender.js'], shell=True)
    return

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:join_sal_gender', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    name_gender = doc.entity('dat:name_gender_lookup', {prov.model.PROV_LABEL:'Name Gender Lookup', prov.model.PROV_TYPE:'ont:DataSet'})

    # 2013 earnings computation
    earnings_2013 = doc.entity('dat:earnings_2013', {prov.model.PROV_LABEL:'Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataSet'}) 
    join_on_name_2013 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Join Salary and Gender on Name 2013', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(join_on_name_2013, this_script)
    doc.usage(join_on_name_2013, earnings_2013, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(join_on_name_2013, name_gender, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    sal_gender_joined_2013 = doc.entity('dat:earnings_2013_combined', {prov.model.PROV_LABEL:'Average Salary Computed by Gender 2013', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(sal_gender_joined_2013, this_script)
    doc.wasGeneratedBy(sal_gender_joined_2013, join_on_name_2013, endTime)
    doc.wasDerivedFrom(sal_gender_joined_2013, earnings_2013, join_on_name_2013, join_on_name_2013, join_on_name_2013)
    doc.wasDerivedFrom(sal_gender_joined_2013, name_gender, join_on_name_2013, join_on_name_2013, join_on_name_2013)

    # 2014 earnings computation
    earnings_2014 = doc.entity('dat:earnings_2014', {prov.model.PROV_LABEL:'Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataSet'}) 
    join_on_name_2014 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Join Salary and Gender on Name 2014', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(join_on_name_2014, this_script)
    doc.usage(join_on_name_2014, earnings_2014, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(join_on_name_2014, name_gender, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    sal_gender_joined_2014 = doc.entity('dat:earnings_2014_combined', {prov.model.PROV_LABEL:'Average Salary Computed by Gender 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(sal_gender_joined_2014, this_script)
    doc.wasGeneratedBy(sal_gender_joined_2014, join_on_name_2014, endTime)
    doc.wasDerivedFrom(sal_gender_joined_2014, earnings_2014, join_on_name_2014, join_on_name_2014, join_on_name_2014)
    doc.wasDerivedFrom(sal_gender_joined_2014, name_gender, join_on_name_2014, join_on_name_2014, join_on_name_2014)

    return doc
    