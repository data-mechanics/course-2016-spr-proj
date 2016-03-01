import gender_sal_data
import join_sal_gender
import sal_avg_by_gender
import prov.model
import pymongo
import json
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

doc = prov.model.ProvDocument()

startTime, _, endTime = gender_sal_data.run()
doc.update(gender_sal_data.to_prov(startTime, endTime))

startTime, _, endTime = join_sal_gender.run()
doc.update(join_sal_gender.to_prov(startTime, endTime))

startTime, _, endTime = sal_avg_by_gender.run()
doc.update(sal_avg_by_gender.to_prov(startTime, endTime))

repo = get_auth_repo('nikolaj', 'nikolaj')
repo.record(doc.serialize()) # Record the provenance document.
# print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()
