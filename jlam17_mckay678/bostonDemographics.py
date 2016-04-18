import urllib.request
import json
import pymongo
import prov.model
import datetime
import uuid

Files = ['Boston_Age',\
        'Boston_BirthLocation',\
        'Boston_Commute',\
        'Boston_Education',\
        'Boston_FamilyIncome',\
        'Boston_GeographicMobility',\
        'Boston_GroupQuarters',\
        'Boston_Household',\
        'Boston_HouseholdIncome', \
        'Boston_HousingTenure',\
        'Boston_HousingUnits',\
        'Boston_IncomePerCapita',\
        'Boston_Industries(1-2)',\
        'Boston_Industries(2-2)',\
        'Boston_LanguageSpoken',\
        'Boston_LFP(1-6)',\
        'Boston_LFP(2-6)',\
        'Boston_LFP(3-6)',\
        'Boston_LFP(4-6)',\
        'Boston_LFP(5-6)',\
        'Boston_LFP(6-6)',\
        'Boston_Naivity',\
        'Boston_Occupation',\
        'Boston_PopulationGrowth',\
        'Boston_RaceEthnicity',\
        'Boston_SchoolEnrollment',\
        'Boston_TravelTime',\
        'Boston_VacancyRates',\
        'Boston_VehiclesPerHousehold',\
        'Boston_WorkPlace'
        ]
agg = [{
        "United States": {},\
        "Massachusetts": {},\
        "Boston": {},\
        "Allston/Brighton": {},\
        "Back Bay": {},\
        "Central": {}, \
        "Charlestown": {}, \
        "East Boston": {}, \
        "Fenway/Kenmore": {}, \
        "Harbor Islands": {}, \
        "Hyde Park": {}, \
        "Jamaica Plain": {}, \
        "Mattapan": {}, \
        "North Dorchester": {},\
        "Roslindale": {}, \
        "Roxbury": {}, \
        "South Boston": {}, \
        "South Dorchester": {}, \
        "South End": {}, \
        "West Roxbury": {}
        }]

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())

# Set up the database connection.
client = pymongo.MongoClient()
repo = client.repo
repo.authenticate('jlam17_mckay678', 'jlam17_mckay678')

# Retrieve some data sets (not using the API here for the sake of simplicity).
startTime = datetime.datetime.now()

filen = 'data/Boston_Age.json'
res = open(filen, 'r')
r1 = json.load(res)
repo.dropPermanent("Boston_Age")
repo.createPermanent("Boston_Age")
repo['jlam17_mckay678.Boston_Age'].insert_many(r1)

filen = 'data/Boston_BirthLocation.json'
res = open(filen, 'r')
r2 = json.load(res)
repo.dropPermanent("Boston_BirthLocation")
repo.createPermanent("Boston_BirthLocation")
repo['jlam17_mckay678.Boston_BirthLocation'].insert_many(r2)

filen = 'data/Boston_Commute.json'
res = open(filen, 'r')
r3 = json.load(res)
repo.dropPermanent("Boston_Commute")
repo.createPermanent("Boston_Commute")
repo['jlam17_mckay678.Boston_Commute'].insert_many(r3)

filen = 'data/Boston_Education.json'
res = open(filen, encoding="utf8")
r4 = json.load(res)
repo.dropPermanent("Boston_Education")
repo.createPermanent("Boston_Education")
repo['jlam17_mckay678.Boston_Education'].insert_many(r4)

filen = 'data/Boston_FamilyIncome.json'
res = open(filen, encoding="utf8")
r5 = json.load(res)
repo.dropPermanent("Boston_FamilyIncome")
repo.createPermanent("Boston_FamilyIncome")
repo['jlam17_mckay678.Boston_FamilyIncome'].insert_many(r5)

filen = 'data/Boston_GeographicMobility.json'
res = open(filen, encoding="utf8")
r6 = json.load(res)
repo.dropPermanent("Boston_GeographicMobility")
repo.createPermanent("Boston_GeographicMobility")
repo['jlam17_mckay678.Boston_GeographicMobility'].insert_many(r6)

filen = 'data/Boston_GroupQuarters.json'
res = open(filen, encoding="utf8")
r7 = json.load(res)
repo.dropPermanent("Boston_GroupQuarters")
repo.createPermanent("Boston_GroupQuarters")
repo['jlam17_mckay678.Boston_GroupQuarters'].insert_many(r7)

filen = 'data/Boston_Household.json'
res = open(filen, encoding="utf8")
r8 = json.load(res)
repo.dropPermanent("Boston_Household")
repo.createPermanent("Boston_Household")
repo['jlam17_mckay678.Boston_Household'].insert_many(r8)

filen = 'data/Boston_HouseholdIncome.json'
res = open(filen, encoding="utf8")
r9 = json.load(res)
repo.dropPermanent("Boston_HouseholdIncome")
repo.createPermanent("Boston_HouseholdIncome")
repo['jlam17_mckay678.Boston_HouseholdIncome'].insert_many(r9)

filen = 'data/Boston_HousingTenure.json'
res = open(filen, encoding="utf8")
r10 = json.load(res)
repo.dropPermanent("Boston_HousingTenure")
repo.createPermanent("Boston_HousingTenure")
repo['jlam17_mckay678.Boston_HousingTenure'].insert_many(r10)

filen = 'data/Boston_HousingUnits.json'
res = open(filen, encoding="utf8")
r11 = json.load(res)
repo.dropPermanent("Boston_HousingUnits")
repo.createPermanent("Boston_HousingUnits")
repo['jlam17_mckay678.Boston_HousingUnits'].insert_many(r11)

filen = 'data/Boston_IncomePerCapita.json'
res = open(filen, encoding="utf8")
r12 = json.load(res)
repo.dropPermanent("Boston_IncomePerCapita")
repo.createPermanent("Boston_IncomePerCapita")
repo['jlam17_mckay678.Boston_IncomePerCapita'].insert_many(r12)

filen = 'data/Boston_Industries(1-2).json'
res = open(filen, encoding="utf8")
r13 = json.load(res)
repo.dropPermanent("Boston_Industries(1-2)")
repo.createPermanent("Boston_Industries(1-2)")
repo['jlam17_mckay678.Boston_Industries(1-2)'].insert_many(r13)

filen = 'data/Boston_Industries(2-2).json'
res = open(filen, encoding="utf8")
r14 = json.load(res)
repo.dropPermanent("Boston_Industries(2-2)")
repo.createPermanent("Boston_Industries(2-2)")
repo['jlam17_mckay678.Boston_Industries(2-2)'].insert_many(r14)

filen = 'data/Boston_LanguageSpoken.json'
res = open(filen, encoding="utf8")
r15 = json.load(res)
repo.dropPermanent("Boston_LanguageSpoken")
repo.createPermanent("Boston_LanguageSpoken")
repo['jlam17_mckay678.Boston_LanguageSpoken'].insert_many(r15)

filen = 'data/Boston_LFP(1-6).json'
res = open(filen, encoding="utf8")
r16 = json.load(res)
repo.dropPermanent("Boston_LFP(1-6)")
repo.createPermanent("Boston_LFP(1-6)")
repo['jlam17_mckay678.Boston_LFP(1-6)'].insert_many(r16)

filen = 'data/Boston_LFP(2-6).json'
res = open(filen, encoding="utf8")
r17 = json.load(res)
repo.dropPermanent("Boston_LFP(2-6)")
repo.createPermanent("Boston_LFP(2-6)")
repo['jlam17_mckay678.Boston_LFP(2-6)'].insert_many(r17)

filen = 'data/Boston_LFP(3-6).json'
res = open(filen, encoding="utf8")
r18 = json.load(res)
repo.dropPermanent("Boston_LFP(3-6)")
repo.createPermanent("Boston_LFP(3-6)")
repo['jlam17_mckay678.Boston_LFP(3-6)'].insert_many(r18)

filen = 'data/Boston_LFP(4-6).json'
res = open(filen, encoding="utf8")
r19 = json.load(res)
repo.dropPermanent("Boston_LFP(4-6)")
repo.createPermanent("Boston_LFP(4-6)")
repo['jlam17_mckay678.Boston_LFP(4-6)'].insert_many(r19)

filen = 'data/Boston_LFP(5-6).json'
res = open(filen, encoding="utf8")
r20 = json.load(res)
repo.dropPermanent("Boston_LFP(5-6)")
repo.createPermanent("Boston_LFP(5-6)")
repo['jlam17_mckay678.Boston_LFP(5-6)'].insert_many(r20)

filen = 'data/Boston_LFP(6-6).json'
res = open(filen, encoding="utf8")
r21 = json.load(res)
repo.dropPermanent("Boston_LFP(6-6)")
repo.createPermanent("Boston_LFP(6-6)")
repo['jlam17_mckay678.Boston_LFP(6-6)'].insert_many(r21)

filen = 'data/Boston_Naivity.json'
res = open(filen, encoding="utf8")
r22 = json.load(res)
repo.dropPermanent("Boston_Naivity")
repo.createPermanent("Boston_Naivity")
repo['jlam17_mckay678.Boston_Naivity'].insert_many(r22)

filen = 'data/Boston_Occupation.json'
res = open(filen, encoding="utf8")
r23 = json.load(res)
repo.dropPermanent("Boston_Occupation")
repo.createPermanent("Boston_Occupation")
repo['jlam17_mckay678.Boston_Occupation'].insert_many(r23)

filen = 'data/Boston_PopulationGrowth.json'
res = open(filen, encoding="utf8")
r24 = json.load(res)
repo.dropPermanent("Boston_PopulationGrowth")
repo.createPermanent("Boston_PopulationGrowth")
repo['jlam17_mckay678.Boston_PopulationGrowth'].insert_many(r24)

filen = 'data/Boston_RaceEthnicity.json'
res = open(filen, encoding="utf8")
r25 = json.load(res)
repo.dropPermanent("Boston_RaceEthnicity")
repo.createPermanent("Boston_RaceEthnicity")
repo['jlam17_mckay678.Boston_RaceEthnicity'].insert_many(r25)

filen = 'data/Boston_SchoolEnrollment.json'
res = open(filen, encoding="utf8")
r26 = json.load(res)
repo.dropPermanent("Boston_SchoolEnrollment")
repo.createPermanent("Boston_SchoolEnrollment")
repo['jlam17_mckay678.Boston_SchoolEnrollment'].insert_many(r26)

filen = 'data/Boston_TravelTime.json'
res = open(filen, encoding="utf8")
r27 = json.load(res)
repo.dropPermanent("Boston_TravelTime")
repo.createPermanent("Boston_TravelTime")
repo['jlam17_mckay678.Boston_TravelTime'].insert_many(r27)

filen = 'data/Boston_VacancyRates.json'
res = open(filen, encoding="utf8")
r28 = json.load(res)
repo.dropPermanent("Boston_VacancyRates")
repo.createPermanent("Boston_VacancyRates")
repo['jlam17_mckay678.Boston_VacancyRates'].insert_many(r28)

filen = 'data/Boston_VehiclesPerHousehold.json'
res = open(filen, encoding="utf8")
r29 = json.load(res)
repo.dropPermanent("Boston_VehiclesPerHousehold")
repo.createPermanent("Boston_VehiclesPerHousehold")
repo['jlam17_mckay678.Boston_VehiclesPerHousehold'].insert_many(r29)

filen = 'data/Boston_WorkPlace.json'
res = open(filen, encoding="utf8")
r30 = json.load(res)
repo.dropPermanent("Boston_WorkPlace")
repo.createPermanent("Boston_WorkPlace")
repo['jlam17_mckay678.Boston_WorkPlace'].insert_many(r30)

for index, filename in enumerate(Files):
    with open("data/" + filename + ".json") as f:
        _file = json.loads("".join([line.strip() for line in f]))[0]
        key = Files[index].split("_",1)[1]
        for k in _file:
            agg[0][k].update({key: _file[k]})

with open('data/Boston_Demographics.json', 'w+') as outfile:
    json.dump(agg, outfile)

filen = 'data/Boston_Demographics.json'
res = open(filen, encoding="utf8")
r31 = json.load(res)
repo.dropPermanent("Boston_Demographics")
repo.createPermanent("Boston_Demographics")
repo['jlam17_mckay678.Boston_Demographics'].insert_many(r31)

endTime = datetime.datetime.now()

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jlam17_mckay678/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/jlam17_mckay678/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:bostonDemographics', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Boston Demographics', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Query':'?accessType=NONE'})
doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, resource, startTime)

for dataSet in Files:
    entity = doc.entity('dat:' + dataSet, {prov.model.PROV_LABEL: dataSet, prov.model.PROV_TYPE: dataSet})
    doc.wasAttributedTo(entity, this_script)
    doc.wasGeneratedBy(entity, this_run, endTime)
    doc.wasDerivedFrom(entity, resource, this_run, this_run, this_run)

repo.record(doc.serialize()) # Record the provenance document.
provEx = open('provBostonDemographics.json', 'w')
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
repo.logout()

## eof

