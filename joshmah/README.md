##Assignment
I chose three data sets pulled from the city of boston website. These three data sets include:


1)Location of Hospitals in Boston

2)Waze Jam Data of Streets

3)EMS 911 Dispatch

Databases Created:
Hospitals and if they are located on any recorded Waze Trafficjam streets, and the number of times they show up.

EMS 911 Dispatch time compared to Waze Jam time, and the number of times that have same hour and minute and show up.


##Future Goals

My goal is to answer the location of nearby hospitals and their relationship to traffic jams, attempting to create
a more efficient way of getting to hospitals. I want to be able to show a positive or negative correlation between certain hospitals and the occurence of traffic jams. 

The second data set can show if there is a typical time that EMS times are dispatched, and if they are on a similar time to traffic jam time. I want to be able to show a positive or negative correlation between ambulances and traffic jams issues. This second data set needs to be improved to answer the question because ambulance data doesn't show where they are going, and there is so much traffic in Boston that it might be in another part of the city. One thing we can use this dataset is to see if ambulances are typical during traffic jam times.

##Getting First database:


Run proj1.py

This file is currently creating a database, that will have number of intersections between hospitals and jams.

Eventually with more jam data, we will have more intersections, but we are able to get pretty significant results. From 1000 traffic jam reports, we have 371 of those occuring on streets that hosptials are located on. 

Inside of proj1.py, I created two different dictionaries that can be helpful later on. One of them lists streets with number of hospitals located on that street, and the another one lists which streets have which hospitals, which can be helpful for parsing. This database can be used by: db.joshmah.intersectionsHospitalsStreets.find();

Currently the file creates a database with streets with hospitals and the number of traffic jams that have occured on it, found using
```
>db.joshmah.intersectionsJamHospitals.find();
```
  
The provenance for this is recorded in plan.json. 


##Getting Second database:

Run proj1partb.py

This file is currently creating a database that has the number of intersections between time in hours/minutes between ambulances and traffic jams reported on Waze, found using
```
>db.joshmah.intersectionsJamAmbulances.find();
```
This data is not as successful, because it includes all traffic jam data inside Boston, with no reference to where the ambulance is located, but there are around 506 intersections out of a database of 1000. 

The provenance for this is recorded in plan2.json

##The 3 different datasets and their links


Hospitals in Boston - https://data.cityofboston.gov/api/views/46f7-2snz/rows.json

Waze Jam Data of Streets - https://data.cityofboston.gov/api/views/yqgx-2ktq/rows.json

EMS 911 Dispatch Data - https://data.cityofboston.gov/Public-Safety/EMS-911-Dispatch/ak6k-a5up

