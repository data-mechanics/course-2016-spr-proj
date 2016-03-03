Tabor Beaudry
CS:591
Project 1

Run gather.py to aquire data
Run reform.py to assemble data sets

For this project I am starting to explore the potential viability of using drones for medical EMS, or to aid in 
the Boston Police Department's response time. I used the BPD 911 call data set from the city of boston site, along 
with the datasests containing the location of both hospitals and police district stations in Boston. Using this data
I created 2 new data sets containing medical emergencies that could potentially be aided by a quick responding drones
and criminal activity where an eye-in-the-sky could potentially be of use to the Boston Police Department.
I used geopy to get distance across the ground between each station or hospital and the address of the 911 call.
geopy includes a lookup for address to latitude,longitude coordinates, which tends to run fairly slowly. Additionally 
I modified the Location data sets to include a count of how many events that this station or hospital is one of the three 
closest.

