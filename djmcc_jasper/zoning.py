# modules
import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

printResult = True

def run(repo):

    startTime = datetime.datetime.now()

    zipcodes = repo.djmcc_jasper.assessments_2014.distinct('zipcode')

    # COUNTS[0] is ptypeList, COUNTS[1] is useList

    assessments_2014 = repo.djmcc_jasper.assessments_2014
    assessments_2015 = repo.djmcc_jasper.assessments_2015

    for zipcode in zipcodes:

        counts2014   = count(assessments_2014, zipcode)
        percents2014 = percents(counts2014[1], zipcode, "2014")

        counts2015   = count(assessments_2015, zipcode)
        percents2015 = percents(counts2015[1], zipcode, "2015")

        post = {"zipcode": zipcode,
                    "2014_counts": {
                        "multiple use": counts2014[1][0],
                        "residential": counts2014[1][1],
                        "apartment": counts2014[1][2],
                        "commercial": counts2014[1][3],
                        "industrial": counts2014[1][4],
                        "exempt": counts2014[1][5],
                    },
                    "2015_counts": {
                        "multiple use": counts2015[1][0],
                        "residential": counts2015[1][1],
                        "apartment": counts2015[1][2],
                        "commercial": counts2015[1][3],
                        "industrial": counts2015[1][4],
                        "exempt": counts2015[1][5],
                    },
                    "2014_percents": {
                        "multiple use": percents2014[0],
                        "residential": percents2014[1],
                        "apartment": percents2014[2],
                        "commercial": percents2014[3],
                        "industrial": percents2014[4],
                        "exempt": percents2014[5],
                    },
                    "2015_percents": {
                        "multiple use": percents2015[0],
                        "residential": percents2015[1],
                        "apartment": percents2015[2],
                        "commercial": percents2015[3],
                        "industrial": percents2015[4],
                        "exempt": percents2015[5],
                    }
                }

        repo.djmcc_jasper.neighborhood_zoning.insert_one(post)

        if printResult:
            printResults(zipcode, counts2014[1], percents2014, counts2015[1], percents2015)

            #Ptype Counts
            #print(zipcode+"\t\n")
            #print(counts2014[0])
            #print(counts2015[0])

    endTime = datetime.datetime.now()
    makeProv(startTime,endTime,repo)

# ANALYSIS

def count(assessments, zipcode):

    # List of all the property types and counts: [[use,ptype1,count],[use,ptype2,count],...]
    ptypeList = []

    # For all docs in the current assessments
    for doc in assessments.find({'zipcode':zipcode}):
        ptype = doc['ptype']

        nameArray = [x[1] for x in ptypeList]

        # If that pytpe is not already in ptypeList, add it with count 1
        if ptype not in nameArray:
            ptypeList.append([doc['use'],ptype,1])
        else:
            #Otherwise, increment the count for that ptype
            for tup in ptypeList:
                if tup[1] == ptype:
                    tup[2] += 1
                    break

    multipleUseCount = 0
    residentialCount = 0
    apartmentCount = 0
    commercialCount = 0
    industrialCount = 0
    exemptCount = 0

    # For each use in [use,ptype1,count], combine all the counts
    for arr in ptypeList:
        if arr[0] == 'MULTIPLE USE PROPERTY':
            multipleUseCount += arr[2]
        elif arr[0] == 'RESIDENTIAL PROPERTY':
            residentialCount += arr[2]
        elif arr[0] == 'APARTMENT PROPERTY':
            apartmentCount += arr[2]
        elif arr[0] == 'COMMERCIAL PROPERTY':
            commercialCount += arr[2]
        elif arr[0] == 'INDUSTRIAL PROPERTY':
            industrialCount += arr[2]
        elif arr[0] == 'EXEMPT OWNERSHIP':
            exemptCount += arr[2]
        elif arr[0] == 'EXEMPT PROPERTY TYPE':
            exemptCount += arr[2]
        else:
            # THIS DOES HAPPEN: NO PROPERTY USE IN JSON FILE
            # (probably because there was no associated code. we should drop all code-less properties)
            continue

    useCounts = [multipleUseCount,residentialCount,apartmentCount,commercialCount,industrialCount,exemptCount]

    # Return both all the ptype counts, and the use counts
    return [sorted(ptypeList, key=lambda x: x[1]),useCounts]

def percents(counts,zipcode,year):
    [multipleUseCount,residentialCount,apartmentCount,commercialCount,industrialCount,exemptCount] = counts

    try:
        allCounted = multipleUseCount + residentialCount + apartmentCount + commercialCount + industrialCount + exemptCount
        multipleUsePercent = (multipleUseCount/float(allCounted))
        residentialPercent = (residentialCount/float(allCounted))
        apartmentPercent = (apartmentCount/float(allCounted))
        commercialPercent = (commercialCount/float(allCounted))
        industrialPercent = (industrialCount/float(allCounted))
        exemptPercent = (exemptCount/float(allCounted))
        return [multipleUsePercent, residentialPercent, apartmentPercent, commercialPercent, industrialPercent, exemptPercent]
    except ZeroDivisionError:
        print(zipcode+"\tNo counts for zipcode " + zipcode + " in year " + year)
        return [0,0,0,0,0,0]

# PRINTERS

def printResults(zipcode, counts2014, percents2014, counts2015, percents2015):

    changeInCounts = [counts2015[0]-counts2014[0],counts2015[1]-counts2014[1],counts2015[2]-counts2014[2],
                      counts2015[3]-counts2014[3],counts2015[4]-counts2014[4],counts2015[5]-counts2014[5]]

    print("\n\t2014\n\t---------------------------------------------------------------")
    printYear(zipcode, counts2014,percents2014)

    print("\t2015\n\t---------------------------------------------------------------")
    printYear(zipcode, counts2015,percents2015)

    print("\tPercent change year-over-year\n\t---------------------------------------------------------------")
    # DIVISION BY ZERO ERROR
    try:
        print(zipcode+"\tmultipleUseChange:\t" + '{:.3%}'.format(changeInCounts[0]/float(counts2014[0])))
    except ZeroDivisionError:
        print(zipcode+"\tmultipleUseChange:\t" + 'N/A')
    try:
        print(zipcode+"\tresidentialChange:\t" + '{:.3%}'.format(changeInCounts[1]/float(counts2014[1])))
    except ZeroDivisionError:
        print(zipcode+"\tresidentialChange:\t" + 'N/A')
    try:
        print(zipcode+"\tapartmentChange:\t" + '{:.3%}'.format(changeInCounts[2]/float(counts2014[2])))
    except ZeroDivisionError:
        print(zipcode+"\tapartmentChange:\t" + 'N/A')
    try:
        print(zipcode+"\tcommercialChange:\t" + '{:.3%}'.format(changeInCounts[3]/float(counts2014[3])))
    except ZeroDivisionError:
        print(zipcode+"\tcommercialChange:\t" + 'N/A')
    try:
        print(zipcode+"\tindustrialChange:\t" + '{:.3%}'.format(changeInCounts[4]/float(counts2014[4])))
    except ZeroDivisionError:
        print(zipcode+"\tindustrialChange:\t" + 'N/A')
    try:
        print(zipcode+"\texemptChange:\t\t" + '{:.3%}'.format(changeInCounts[5]/float(counts2014[5]))+"\n\n")
    except ZeroDivisionError:
        print(zipcode+"\texemptChange:\t\t" + "N/A \n\n")

def printYear(zipcode, counts,percents):

    [multipleUseCount,residentialCount,apartmentCount,commercialCount,industrialCount,exemptCount] = counts
    [multipleUsePercent, residentialPercent, apartmentPercent, commercialPercent, industrialPercent, exemptPercent] = percents

    print(zipcode+"\tmultipleUseCount:\t" + str(multipleUseCount) + "\tmultipleUsePercent:\t" + '{:.3%}'.format(multipleUsePercent))
    print(zipcode+"\tresidentialCount:\t" + str(residentialCount) + " \tresidentialPercent:\t" + '{:.3%}'.format(residentialPercent))
    print(zipcode+"\tapartmentCount:\t\t" + str(apartmentCount) + "\tapartmentPercent:\t" + '{:.3%}'.format(apartmentPercent))
    print(zipcode+"\tcommercialCount:\t" + str(commercialCount) + "\tcommercialPercent:\t" + '{:.3%}'.format(commercialPercent))
    print(zipcode+"\tindustrialCount:\t" + str(industrialCount) + "\tindustrialPercent:\t" + '{:.3%}'.format(industrialPercent))
    print(zipcode+"\texemptCount:\t\t" + str(exemptCount) + "\texemptPercent:\t\t" + '{:.3%}'.format(exemptPercent) + "\n")

def makeProv(startTime,endTime,repo):
    # Create the provenance document describing everything happening
    # in this script. Each run of the script will generate a new
    # document describing that invocation event. This information
    # can then be used on subsequent runs to determine dependencies
    # and "replay" everything. The old documents will also act as a
    # log.

    doc = prov.model.ProvDocument()

    ########## NAMESPACES
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/') # The scripts in <folder>/<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/') # The data sets in <user>/<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    ########## AGENTS: (this script)
    this_script = doc.agent('alg:zoning', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    ########## ENTITIES: (mongodb databases we're loading)
    resource_2014 = doc.entity('dat:assessments_2014', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
    resource_2015 = doc.entity('dat:assessments_2015', {'prov:label':'Property Assessments 2015', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

    ########## ACTIVITY: RETRIEVE
    retrieve_2014  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})
    retrieve_2015  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Retrieval'})

    doc.wasAssociatedWith(retrieve_2014, this_script)
    doc.wasAssociatedWith(retrieve_2015, this_script)

    doc.used(retrieve_2014, resource_2014, startTime)
    doc.used(retrieve_2015, resource_2015, startTime)

    ########## ACTIVITY: COMPUTE
    make_Count_and_Percent_Computations_2014  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
    make_Count_and_Percent_Computations_2015  = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

    doc.wasAssociatedWith(make_Count_and_Percent_Computations_2014, this_script)
    doc.wasAssociatedWith(make_Count_and_Percent_Computations_2015, this_script)

    doc.used(make_Count_and_Percent_Computations_2014, resource_2014, startTime)
    doc.used(make_Count_and_Percent_Computations_2015, resource_2015, startTime)

    ########## ENTITIES: Newly created

    prov_neighborhood_zoning = doc.entity('dat:neighborhood_zoning', {prov.model.PROV_LABEL:'Neighborhood Zoning', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(prov_neighborhood_zoning, this_script)
    doc.wasGeneratedBy(prov_neighborhood_zoning, make_Count_and_Percent_Computations_2014, endTime)
    doc.wasGeneratedBy(prov_neighborhood_zoning, make_Count_and_Percent_Computations_2015, endTime)
    doc.wasDerivedFrom(prov_neighborhood_zoning, resource_2014, make_Count_and_Percent_Computations_2014, make_Count_and_Percent_Computations_2014, make_Count_and_Percent_Computations_2014)
    doc.wasDerivedFrom(prov_neighborhood_zoning, resource_2015, make_Count_and_Percent_Computations_2015, make_Count_and_Percent_Computations_2015, make_Count_and_Percent_Computations_2015)

    repo.record(doc.serialize()) # Record the provenance document.
