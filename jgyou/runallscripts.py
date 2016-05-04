'''
runallscripts.py

central script to run all other scripts

'''

# get some data sets
exec(open("retrievehospitals.py").read())
exec(open("cleanhospitals.py").read())
exec(open("inputmbta.py").read())
exec(open("retrieveservices.py").read())
#exec(open("retrievepharmacies.py").read())		# needs to be changed to find nearest X pharmacies
exec(open("retrievezillow.py").read())			# currently have not figured out way to incorporate into score, also update retrieval query prov accordingly

# calculate scores
exec(open("scorecoordinates.py").read())

# create csv for visualization in D3
exec(open("makescorecsv.py").read())
			

			



