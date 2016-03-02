'''
assumes auth.json file is located in same directory, as are
all relevant scripts.
'''

# needle request data
exec(open("retrievedata.py").read())


# drop off sites + geo lookup + combining coordinates + addresses
exec(open("retrievesites.py").read())
exec(open("retrievesitesgeo.py").read())
exec(open("cleansitesgeo.py").read())
exec(open("mergesitesgeo.py").read())

# info related to hospitals is retrieved and then combined with current sites
exec(open("retrievehospitals.py").read())
exec(open("cleanhospitals.py").read())
#exec(open("mergedropoffhospitals.py").read())

# k-means
exec(open("calculatemeans.py").read())

# requires prov
# cleanhospitals

# included in plan.json
#retrievedata - update
#retrievesitesgeo
#retrievesites
# cleansitesgeo - update
# mergesitesgeo - update
# retrievehosp - update
# calculatemeans - update