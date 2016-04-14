Tabor Beaudry
CS:591

Project 2 WORK IN PROGRESS

The goal of Project 2 is to delve a bit deeper into the use of drones for medical and police services. To solve this
problem I plan on investigating the best way to distribute drones, an optimization problem, and for examining the use
of them at different time of the day, a statistical analysis problem. Medical drones currently in development can have
quite a hefty price associated with them, and with limited resources it will be important to distribute them
effectively. Further, examining the distribution of EMS events across times of day and days of the week can help
fine tune when these devices could really be put to use. Visualizaing this data will involve combining data resulting
from statistical analysis, and more human understandable visualization: heatmaps, and gps pathing comparisons.



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

