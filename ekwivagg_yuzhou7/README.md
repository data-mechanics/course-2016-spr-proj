Erica Wivagg (ekwivagg@bu.edu)
Yu Zhou (yuzhou7@bu.edu)

To run:

run_project.py : will run all associated project 1 scripts:
retrieval.py : retrieves and filters all data from City of Boston opendata
rodent_problems.py : performs a join to determine which restaurants have rodent problems nearby
inspections.py : performs a join to determine which licensed restaurants failed inspection
get_crime.py : retrieves all larceny from vehicle crimes
get_closest_stop.py : finds closest stop for every restaurant
get_liquor.py : finds closest stop for every restaurant with a liquor license


Datasets used:

Project 1:
1. Active Food Establishment Licenses
2. Food Establishment Inspections
3. Mayors 24 Hour Hotline

Project 2:
1. Active Food Establishment Licenses
2. Liquor Licenses
3. Crime Incident Report
4. Location of all T stops, pulled from ciestu_sajarvis


Justification:

Project 1:
The question we are looking to answer is: how safe and healthy are restaurants in Boston?
We will look at all licensed restaurants in Boston and find out whether they had inspections, and if so, whether they passed or failed these inspections (combining (1) and (2) above).
Additionally, we will look at the Mayor's Hotline and determine whether restaurants have had problems with rodents or other issues that are reported.
Future project angles may seek to find crime incident reports that occur near restaurants, violations of liquor licenses, or 311 complaints.
The goal to to determine for any particular restaurant what the unsafe aspects or incidents are that have occurred at or nearby the restaurant.

For this assignment, we will focus on determining what inspections have been performed and passed/failed by each restaurant (using (1) and (2) above) and which restaurants have had rodent problems nearby (using (1) and (3) above).

Project 2:
The focus of our project has shifted to determining several transit-related metrics about Boston restaurants. Specifically, we will be looking to find the closest T stop to every licensed restaurant and using this information to find out how many restaurants have each T stop as their closest stop. This will serve as a kind of "hotspot" locator for T stops - we will be able to show how many restaurants are closer to a given T stop than to any other T stop. In essence we are establishing a "neighborhood" for each T stop and finding how many restaurants are in that neighborhood.
We will use the T stop neighborhoods again to determine how safe a given area is relative to number of liquor-licensed restaurants in the neighborhood. More precisely, we will determine how many restaurants in a given T stop neighborhood have liquor licenses. We will correlate this number to the number of "larceny from vehicle" crimes that occurred in that neighborhood to determine if neighborhoods with more liquor-licensed restaurants are more likely to have vehicle-related crimes. This will provide a metric to determine if it is safe to drive and park at a restaurant or if you should try to take public transit.
Combined with our Project 1 measurements of restaurant safety based on number of rodent incidents and food inspection failures, the ultimate goal (not realized in this project) is to create a score for each restaurant based on how healthy it is (rats and inspection failures), how safe it is to park there (vehicle crimes), and how far it is from the nearest public transit.

Transformations:

Project 1:
We will perform a map/reduce that aggregates Active Food Establishment Licenses with Food Establishment Inspections to determine which currently licensed restaurants have failed inspection.
Our second transformation will perform a map/reduce that aggregates Active Food Establishment Licenses and Mayors 24 Hour Hotline reports to determine if there are any restaurants that have had rodent activity nearby. We aggregate on rounded latitude and longitude, to determine the area immediately surrounding a restaurant.

Project 2:
Find the closest T stop to every restaurant and to every restaurant with a liquor license. For each T stop find the number of restaurants for which the closest T stop is that stop in order to find the number of restaurants in each T stop's neighborhood."
Correlate the number of vehicle larcenies with the number of restaurants that have a liquor license for every given T stop "neighborhood."

Data Visualizations (in progress):

Show a bar graph for each T stop of the number of restaurants in that stop's "neighborhood" 

Show a correlation between number of restaurants in a given "neighborhood" with a liquor license and number of vehicle larcenies in that "neighborhood".
