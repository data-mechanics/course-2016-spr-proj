'''
runallscripts.py

'''




exec(open("convertcoordinates.py").read())
exec(open("retrievehospitals.py").read())
exec(open("cleanhospitals.py").read())
exec(open("inputmbta.py").read())
#exec(open("inputcommcenters.py").read())


startlocation = (-71.0580,42.3604)

fip = getCensus(startlocation)
(addr, neigh, zipcode) = getAddress(startlocation)
