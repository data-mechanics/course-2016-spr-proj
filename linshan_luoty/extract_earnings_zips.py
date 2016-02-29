import datetime
import pymongo
import sys

# Until a library is created, we just use the script directly.
exec(open('../pymongo_dm.py').read())
exec(open('get_repo.py').read())

# Set up the database connection.
db = repo[auth['admin']['name']+'.'+'employee_earnings_report_2014']

startTime = datetime.datetime.now()

# update extract earnings and zips info
earnings_zips = []
for document in db.find({}, {'zip': True,'total_earnings':True, '_id': False}):	# only project zips and earnings
	if 'zip' in document:
		earnings_zips.append( {'zip': document['zip'], 'total_earnings': float(document['total_earnings'])} )

# save it to a temporary folder
repo.dropTemporary("earnings_zips")
repo.createTemporary("earnings_zips")
repo['linshan_luoty.earnings_zips'].insert_many(earnings_zips)

endTime = datetime.datetime.now()
	

repo.logout()
