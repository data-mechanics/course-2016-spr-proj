'''
Description: 
A script that retrieves and stores drop off sites in Boston for needles and other medical sharps

References:
http://stackoverflow.com/questions/14592762/a-good-way-to-get-the-charset-encoding-of-an-http-response-in-python
'''

from bs4 import BeautifulSoup, SoupStrainer
import urllib.request
import json

# get response from page, parses

urlbhpc = "http://www.bphc.org/whatwedo/Addiction-Services/services-for-active-users/Pages/Safe-Needle-and-Syringe-Disposal.aspx"
response = urllib.request.urlopen(urlbhpc)
#response_encoding = response.headers.get_content_charset(default)
#doc = rqst.read()

only_webpart = SoupStrainer(id="WebPartWPQ3")
soup = BeautifulSoup(response, 'html.parser', parse_only=only_webpart)

# find resource name, url, address
for t in (soup.find('table')):
	for td in (t.find_all('td')):
		for s in td.find_all('strong'):
			print(s.string.encode('utf-8'))
		for a in td.find_all('a'):
			link = a.get('href')
			print(link)
		for b in td.find_all('div'):
			if b.string != None:
				print(b.string)
		print()
#print(soup.prettify().encode('utf-8'))