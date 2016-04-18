'''
Nicholas Louie, Thomas Hsu
nlouie@bu.edu, thsu@bu.edu
nlouie_thsu8
4/13/16
Boston University Department of Computer Science
CS 591 L1 - Data Mechanics Project 2
Andrei Lapets (lapets@bu.edu)
Datamechanics.io

'''


import json

'''
c_f = open('crimes_night.json')
c_j = json.load(c_f)
c_f.close()
c_f = open('crimes_night.txt', 'w')
for c in c_j:
	s = '%s\t%s\t%s\t%s\t%s\n' % (c['sid'], c['Location']['latitude'], c['Location']['longitude'], c['FROMDATE'], c['INCIDENT_TYPE_DESCRIPTION'])
	c_f.write(s)
'''

l_f = open('lights.json')
l_j = json.load(l_f)
l_f.close()
l_f = open('lights.txt', 'w')
for l in l_j:
    s = '%s\t%s\t%s\n' % (l['OBJECTID'], l['Lat'], l['Long'])
    l_f.write(s)
