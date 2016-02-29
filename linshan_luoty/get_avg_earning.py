import datetime
import pymongo
import sys
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
repo.dropTemporary("zip_avg_earnings")
repo.createTemporary("zip_avg_earnings")
repo['linshan_luoty.zip_avg_earnings'].insert_many(zip_avg_earnings)


endTime = datetime.datetime.now()
	

repo.logout()