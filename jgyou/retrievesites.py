'''
Description: 
A script that retrieves and stores drop off sites in Boston for needles and other medical sharps

References:
http://stackoverflow.com/questions/14592762/a-good-way-to-get-the-charset-encoding-of-an-http-response-in-python
http://stackoverflow.com/questions/15321138/removing-unicode-u2026-like-characters-in-a-string-in-python2-7
http://kaira.sgo.fi/2014/05/saving-and-loading-data-in-python-with.html
http://stackoverflow.com/questions/12309269/how-do-i-write-json-data-to-a-file-in-python
'''

from bs4 import BeautifulSoup, SoupStrainer
import urllib.request
import json
import re

import pymongo
import prov.model
import datetime
import uuid

exec(open('../pymongo_dm.py').read())

client = pymongo.MongoClient()
repo = client.repo

f = open("auth.json").read()
auth = json.loads(f)
user = auth['user']

repo.authenticate(user, user)

startTime = datetime.datetime.now()

# get response from page, parses page to get information

urlbhpc = "http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx"
response = urllib.request.urlopen(urlbhpc)
#response_encoding = response.headers.get_content_charset(default)

only_webpart = SoupStrainer(id="WebPartWPQ3")
soup = BeautifulSoup(response, 'html.parser', parse_only=only_webpart)

resources = []

# find resource name, url, address
for t in (soup.find('table')):

	for td in (t.find_all('td')):
		
		name, addr, phone, link, addr1, town, zipcode = "", "", "", None, "", "", "" 

		# resource name
		for s in td.find_all('strong'):
			temp = str(s.string)
			name = temp.encode('ascii','ignore').decode('utf-8')
			#print(s.string.decode('unicode_escape').encode('ascii','ignore'))
		
		# link
		for a in td.find_all('a'):
			link = a.get('href')
			#print(link)

		for b in td.find_all('div'):
			if b.string != None:
				temp2 = str(b.string)

				# phone number
				if re.match(r"[0-9][0-9][0-9]-[0-9]", temp2) != None:
					phone = temp2.strip()
				else:
					# save street name, town, zipcode
					if re.match(r"([A-Za-z]+)(.*), MA", temp2) != None:
						temp3 = temp2.split()
						town = re.sub(r"[^A-Za-z-]", "", temp3[0])
						for e in temp3:
							if re.match(r"[0-9]+", e):
								zipcode = e
					else: 
						addr1 = temp2.strip()
					# save overall address for reference
					addr = addr + temp2.strip() + " "

		resources.append({"resource_name": str(name), "location": addr, "location_street_name": addr1, "neighborhood": town, "location_zipcode": zipcode, "phone": phone, "weburl": link})


repo.dropTemporary("currentsites")
repo.createTemporary("currentsites")
repo[user + '.currentsites'].insert_many(resources)

endTime = datetime.datetime.now()



# record provenance data

provdoc = prov.model.ProvDocument()
provdoc.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc.add_namespace('bhpc', 'http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/')	# Boston Public Health website.

runid = str(uuid.uuid4())
this_script = provdoc.agent('alg:retrievesites', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc.entity('bhpc:safeneedle', {'prov:label':'Safe Needle and Syringe Disposal Webpage', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'aspx'})
this_run = provdoc.activity('log:a'+runid, startTime, endTime)
provdoc.wasAssociatedWith(this_run, this_script)
provdoc.used(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'Safe-Needle-and-Syringe-Disposal.aspx'})

dropoffsites = provdoc.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc.wasAttributedTo(dropoffsites, this_script)
provdoc.wasGeneratedBy(dropoffsites, this_run, endTime)
provdoc.wasDerivedFrom(dropoffsites, resource, this_run, this_run, this_run)

repo.record(provdoc.serialize()) # Record the provenance document.

# add to plan.json

provdoc2 = prov.model.ProvDocument()
provdoc2.add_namespace('alg', 'http://datamechanics.io/algorithm/' + user + '/') # The scripts in <folder>/<filename> format.
provdoc2.add_namespace('dat', 'http://datamechanics.io/data/' + user + '/') # The data sets in <user>/<collection> format.
provdoc2.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
provdoc2.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
provdoc2.add_namespace('bhpc', 'http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/')	# Boston Public Health website.


this_script = provdoc2.agent('alg:retrievesites', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = provdoc2.entity('bhpc:safeneedle', {'prov:label':'Safe Needle and Syringe Disposal Webpage', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'aspx'})
this_run = provdoc2.activity('log:a'+runid)
provdoc2.wasAssociatedWith(this_run, this_script)
provdoc2.used(this_run, resource, None, None, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'Safe-Needle-and-Syringe-Disposal.aspx'})

dropoffsites = provdoc2.entity('dat:currentsites', {prov.model.PROV_LABEL:'Current Drop-Off Sites', prov.model.PROV_TYPE:'ont:DataSet'})
provdoc2.wasAttributedTo(dropoffsites, this_script)
provdoc2.wasGeneratedBy(dropoffsites, this_run)
provdoc2.wasDerivedFrom(dropoffsites, resource, this_run, this_run, this_run)

plan = open('plan.json','r')
docModel = prov.model.ProvDocument()
doc2 = docModel.deserialize(plan)
doc2.update(provdoc2)
plan.close()
plan = open('plan.json', 'w')
plan.write(json.dumps(json.loads(doc2.serialize()), indent=4))
plan.close()

repo.logout()
