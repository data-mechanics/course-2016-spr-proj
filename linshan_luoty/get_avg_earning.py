import json
import datetime
import pymongo
import prov.model
import uuid
from bson.code import Code

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

# Set up the database connection.
db = repo[auth['admin']['name']+'.'+'earnings_zips']

'''
The following MapReduce method doesn't work because of authorization problem.

mapper = Code("""
	function () {
		emit(this.zip, 1);
	}
	""")

reducer = Code("""
	function (key, values) {
		var total = 0;
		for (var i = 0; i < values.length; i++) {
			total += values[i];
		}
		return total;
	}
	""")

#result = db.map_reduce(mapper, reducer, "zip_avg_earnings")
'''


startTime = datetime.datetime.now()

# calculate the total earnings for each zipcode
pipeline = [{"$group": {"_id": "$zip", "total_earnings": {"$sum": "$total_earnings"}, "total_residents": {"$sum": 1}}}]

zip_total_earnings = list(db.aggregate(pipeline))

# calculate avg earnings
zip_avg_earnings = [ {'zip': d['_id'], 'avg_earning': d['total_earnings']/d['total_residents']} for d in zip_total_earnings ]

# save it to a temporary folder
repo.dropPermanent("zip_avg_earnings")
repo.createPermanent("zip_avg_earnings")
repo['linshan_luoty.zip_avg_earnings'].insert_many(zip_avg_earnings)

endTime = datetime.datetime.now()
	

# Create the provenance document describing everything happening
# in this script. Each run of the script will generate a new
# document describing that invocation event. This information
# can then be used on subsequent runs to determine dependencies
# and "replay" everything. The old documents will also act as a
# log.
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/algorithm/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'https://data-mechanics.s3.amazonaws.com/linshan_luoty/data/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'https://data-mechanics.s3.amazonaws.com/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'https://data-mechanics.s3.amazonaws.com/log#') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:get_avg_earning', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

earning_zip = doc.entity('dat:earnings_zips', {prov.model.PROV_LABEL:'Earnings Zips', prov.model.PROV_TYPE:'ont:DataSet'})

get_avg_earning = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(get_avg_earning, this_script)
doc.usage(get_avg_earning, earning_zip, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

zip_avg_earning = doc.entity('dat:zip_avg_earnings', {prov.model.PROV_LABEL:'Zip Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(zip_avg_earning, this_script)
doc.wasGeneratedBy(zip_avg_earning, get_avg_earning, endTime)
doc.wasDerivedFrom(zip_avg_earning, earning_zip, get_avg_earning, get_avg_earning, get_avg_earning)

repo.record(doc.serialize()) # Record the provenance document.
#print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','a').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())

repo.logout()