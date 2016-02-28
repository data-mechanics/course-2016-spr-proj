'''
assumes auth.json file is located in same directory, as are
all relevant scripts.
'''

exec(open("retrievedata.py").read())
exec(open("retrievesites.py").read())
exec(open("retrievesitesgeo.py").read())
exec(open("cleansitesgeo.py").read())
exec(open("mergesitesgeo.py").read())
#exec(open("calculatemeans.py").read())