Tabor Beaudry
CS:591

Project 2 (poor opt)

requires a rerun of gather.py and reform.py as changes have been made to the corresponding datasets
still takes a long time because it requires geolocates on all poorly labeled data

stat_analysis.py for statistics
medical_drone_opt.py for optimization attempt

index1 and index2 for html

The goal of Project 2 is to delve a bit deeper into the use of drones for medical and police services. To solve this
problem I plan on investigating the best way to distribute drones, an optimization problem, and for examining the use
of them at different time of the day, a statistical analysis problem. Medical drones currently in development can have
quite a hefty price associated with them, and with limited resources it will be important to distribute them
effectively. Further, examining the distribution of EMS events across times of day and days of the week can help
fine tune when these devices could really be put to use. Visualizaing this data will involve combining data resulting
from statistical analysis, and more human understandable visualization: Interactive histogram and Projection of injury data.
index1 shows a interactive histogram that shows the number of calls that could be covered each day in the data set by
using users choice of hospitals as lauching points for drones. index2 shows a rough cartesian projection of data points
by longitude and latitude and a highlighter traces a rougly 1 mile radius over each point. This was intended to be a more
substantial visualization and may still see improvements. Statistically, I found minimal correlation between
time of day and location of both medical events and criminal events. I was hoping to see some correlation here
so that optimizations could take into general location within the city i.e. more drones would be needed near the city center.
However Boston appears to have a dense enough population that combiend with the available data no such trends could be found.
My goal with the optimization was to set up script such that a variety of factors could be changed
in order to be useable on many systems. Unfortunately while in many ways this script could do that, as of yet
I have been unable to get a solver working that can use integer programing and between that and weak boundry information
I was unable to produce a good optimization.




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

