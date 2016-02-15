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
		
		name, addr, phone, link = "", "", "", None 

		for s in td.find_all('strong'):
			temp = str(s.string)
			name = temp.encode('ascii','ignore')
			#print(s.string.decode('unicode_escape').encode('ascii','ignore'))
		for a in td.find_all('a'):
			link = a.get('href')
			#print(link)
		for b in td.find_all('div'):
			if b.string != None:
				temp2 = str(b.string)
				if re.match(r"[0-9][0-9][0-9]-[0-9]", temp2) != None:
					phone = temp2.strip()
				else:
					addr = addr + temp2.strip() + "\n"
				#print(b.string)
		#print()
		resources.append({"resource_name": str(name), "addr": addr, "phone": phone, "link": link})

print(json.dumps(resources, sort_keys = True, indent=4))