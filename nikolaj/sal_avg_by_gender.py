import subprocess
import utils
import prov.model
import uuid

@utils.timestamped
def run():
    subprocess.call(['mongo repo -u nikolaj -p nikolaj --authenticationDatabase "repo" < sal_avg_by_gender.js'], shell=True)
    return

def to_prov(startTime, endTime):
    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/nikolaj/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/nikolaj/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    
    this_script = doc.agent('alg:sal_avg_by_gender', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # computing 2013 salary averages
    ind_sal_gender_2013 = doc.entity('dat:earnings_2013_combined', {prov.model.PROV_LABEL:'Individual Salary by Gender 2013', prov.model.PROV_TYPE:'ont:DataSet'})
    avg_sal_comp_2013 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Compute Average Salary by Gender 2013', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(avg_sal_comp_2013, this_script)
    doc.usage(avg_sal_comp_2013, ind_sal_gender_2013, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    avg_sal_by_gender_2013 = doc.entity('dat:earnings_2013_avg_by_gender', {prov.model.PROV_LABEL:'Average Salary Computed by Gender 2013', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(avg_sal_by_gender_2013, this_script)
    doc.wasGeneratedBy(avg_sal_by_gender_2013, avg_sal_comp_2013, endTime)
    doc.wasDerivedFrom(avg_sal_by_gender_2013, ind_sal_gender_2013, avg_sal_comp_2013, avg_sal_comp_2013, avg_sal_comp_2013)
    
    # computing 2014 salary averages
    ind_sal_gender_2014 = doc.entity('dat:earnings_2014_combined', {prov.model.PROV_LABEL:'Individual Salary by Gender 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    avg_sal_comp_2014 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL:'Compute Average Salary by Gender 2014', prov.model.PROV_TYPE:'Computation'})

    doc.wasAssociatedWith(avg_sal_comp_2014, this_script)
    doc.usage(avg_sal_comp_2014, ind_sal_gender_2014, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    
    avg_sal_by_gender_2014 = doc.entity('dat:earnings_2014_avg_by_gender', {prov.model.PROV_LABEL:'Average Salary Computed by Gender 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(avg_sal_by_gender_2014, this_script)
    doc.wasGeneratedBy(avg_sal_by_gender_2014, avg_sal_comp_2014, endTime)
    doc.wasDerivedFrom(avg_sal_by_gender_2014, ind_sal_gender_2014, avg_sal_comp_2014, avg_sal_comp_2014, avg_sal_comp_2014)
    
    return doc
    