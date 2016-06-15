##########################################################
# Title:   get_apartments.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Scrape address data and retain
#          relevant apartments  
##########################################################

import urllib.request
import json
import dml
import time
import prov.model
import uuid
import datetime

# mapping from common street type strigns to two letter abbreviations

SUFFIXES = {
    'AVE': 'AV',
    'AVENUE': 'AV',
    'BLVD': 'BL',
    'CIRCUIT': 'CC',
    'CIRCLE': 'CI',
    'CRESCENT': 'CR',                
    'CROSSWAY': 'CW',
    'COURT': 'CT',
    'DRIVE': 'DR',
    'HIGHWAY': 'HW',
    'LANE': 'LA',
    'LN': 'LA',
    'PARK': 'PK',
    'PLACE': 'PL',
    'PKWY': 'PW',
    'PARKWAY': 'PW',
    'PLAZA': 'PZ',
    'ROAD': 'RD',
    'ROW': 'RO',
    'SQUARE': 'SQ',
    'STREET': 'ST',
    'TERRACE': 'TE',
    'WAY': 'WA',
    'WAY': 'WY',
    'WHARF': 'WH'
}

# add values as keys (in order to pass checks on suffixes later on)

vs = []
for k in SUFFIXES:
    vs.append(SUFFIXES[k])

for v in vs:
    SUFFIXES[v] = v

# an address is valid if it has a street number, name, and is located in Boston

def isValidAddress(tokens):
    return tokens[0].isnumeric() and 'BOSTON' in tokens and tokens.index('BOSTON') > 1

# main()

def run(repo):

    # for provenance
    startTime = datetime.datetime.now()

    print('Filtering for apartments with a valid Boston address...')

    url_prefix = 'https://www.padmapper.com/show.php?id='
    count = 1
    for apartment in repo.djmcc_jasper.collapsed_apartments.find():
        print('Query #%s: scraping id number %s...' % (count, apartment['id']))
        count += 1
        scrape = urllib.request.urlopen(url_prefix + apartment['id']).read().decode('utf-8').upper()
        tokens = scrape.split('-')

        if 'AT' in tokens and 'MA' in tokens:
            bgn = tokens.index('AT') + 1         # begin slice 1 token later than "AT"
            end = tokens.index('MA') + 2         # end slice 2 tokens later than "MA"
            tokens = tokens[bgn:end]             # retain only the address information in scrape
            zipcode = tokens[len(tokens)-1][0:5] # retain only the first 5 char of zipcode
            suffix = ''

            if isValidAddress(tokens): # if we have a street number and the apartment is in Boston
                number = tokens[0]
                index = tokens.index('BOSTON')
                name = tokens[1:index]

                # handle East and West to conform with assessment data:
                if name[len(name)-1] == 'E':
                    name[len(name)-1] = 'EAST'
                elif name[len(name)-1] == 'W':
                    name[len(name)-1] = 'WEST'

                suffix = name[len(name)-1]
                nameStr = ''
                suffixStr = ''
                if suffix in SUFFIXES:
                    suffixStr = SUFFIXES[suffix]
                    name.pop()                            # remove the suffix from the name
                    for token in name:
                        nameStr += token + ' '
                    nameStr = nameStr[0:len(nameStr)-1]   # remove final trailing space
                else:                 
                    for token in name:                     
                        nameStr += token + ' '
                    nameStr = nameStr[0:len(nameStr)-1]
                address = ''
                if not suffixStr == '':
                    address = number + ' ' + nameStr + ' ' + suffixStr + ' ' + zipcode
                else:
                    address = number + ' ' + nameStr + ' ' + zipcode
                apartment['address'] = address
                apartment['st_num'] = number
                apartment['st_name'] = nameStr 
                apartment['st_name_suf'] = suffixStr
                apartment['zipcode'] = zipcode
                repo.djmcc_jasper.filtered_apartments.insert_one(apartment)
        
        time.sleep(1) # being a good web-citizen

    print('Removed ' + str(repo.djmcc_jasper.collapsed_apartments.count() - repo.djmcc_jasper.filtered_apartments.count()) + ' apartments without valid addresses...')
    
    # for provenance
    endTime = datetime.datetime.now()

    # provenance:

    doc = prov.model.ProvDocument()
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/djmcc_jasper/')
    doc.add_namespace('dat', 'http://datamechanics.io/data/djmcc_jasper/')
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
    doc.add_namespace('log', 'http://datamechanics.io/log#')
    doc.add_namespace('pdm', 'https://www.padmapper.com/')

    # agent: script
    agt_script = doc.agent('alg:clean_apartments', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

    # entity: data source
    res_padmapper = doc.entity('pdm:show', {'prov:label':'Markers', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'php'})

    # activity: hybrid - query and compute (hybrid was discussed at office hours)
    act_query_and_clean = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Hybrid', 'ont:Query':'?id=<id>'})

    # entity: data set
    dat_collapsed_apartments = doc.entity('dat:collapsed_apartments', {prov.model.PROV_LABEL:'Collapsed Apartments', prov.model.PROV_TYPE:'ont:DataSet'})

    # edges
    doc.wasAssociatedWith(act_query_and_clean, agt_script)
    doc.used(act_query_and_clean, res_padmapper, startTime)
    doc.used(act_query_and_clean, dat_collapsed_apartments, startTime)

    # entity: data set
    dat_filtered_apartments = doc.entity('dat:filtered_apartments', {prov.model.PROV_LABEL:'Filtered Apartments', prov.model.PROV_TYPE:'ont:DataSet'})
    
    # edges
    doc.wasAttributedTo(dat_filtered_apartments, agt_script)
    doc.wasGeneratedBy(dat_filtered_apartments, act_query_and_clean, endTime)
    doc.wasDerivedFrom(dat_filtered_apartments, res_padmapper, act_query_and_clean, act_query_and_clean, act_query_and_clean)
    doc.wasDerivedFrom(dat_filtered_apartments, dat_collapsed_apartments, act_query_and_clean, act_query_and_clean, act_query_and_clean)

    repo.record(doc.serialize()) 

#EOF
