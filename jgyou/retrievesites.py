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

# remember to modify this line later
repo.authenticate("jgyou", "jgyou")

# get response from page, parses

urlbhpc = "http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx"
response = urllib.request.urlopen(urlbhpc)
#response_encoding = response.headers.get_content_charset(default)
#doc = rqst.read()

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
			name = temp.encode('ascii','ignore')
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
					if re.match(r"[A-Za-z]+, MA", temp2) != None:
						town, temp3, zipcode = temp2.split()
						i = re.search(",", town)
						town = town[:i.start()]
					else: 
						addr1 = temp2.strip()
					# save overall address for reference
					addr = addr + temp2.strip() + "\n"
				#print(b.string)
		#print()
		resources.append({"resource_name": str(name), "addr": addr, "street": addr1, "town": town, "zipcode": zipcode, "phone": phone, "link": link})

print(json.dumps(resources, sort_keys = True, indent=4))

