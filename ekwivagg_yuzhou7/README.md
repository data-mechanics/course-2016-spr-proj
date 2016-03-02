Erica Wivagg (ekwivagg@bu.edu)
Yu Zhou (yuzhou7@bu.edu)

To run:

run_project.py : will run all associated project 1 scripts:
retrieval.py : retrieves and filters all data from City of Boston opendata
rodent_problems.py : performs a join to determine which restaurants have rodent problems nearby
inspections.py : performs a join to determine which licensed restaurants failed inspection


Datasets used:

1. Active Food Establishment Licenses
2. Food Establishment Inspections
3. Mayors 24 Hour Hotline

Justification:

The question we are looking to answer is: how safe and healthy are restaurants in Boston?
We will look at all licensed restaurants in Boston and find out whether they had inspections, and if so, whether they passed or failed these inspections (combining (1) and (2) above).
Additionally, we will look at the Mayor's Hotline and determine whether restaurants have had problems with rodents or other issues that are reported.
Future project angles may seek to find crime incident reports that occur near restaurants, violations of liquor licenses, or 311 complaints.
The goal to to determine for any particular restaurant what the unsafe aspects or incidents are that have occurred at or nearby the restaurant.

For this assignment, we will focus on determining what inspections have been performed and passed/failed by each restaurant (using (1) and (2) above) and which restaurants have had rodent problems nearby (using (1) and (3) above).

Transformations:

We will perform a map/reduce that aggregates Active Food Establishment Licenses with Food Establishment Inspections to determine which currently licensed restaurants have failed inspection.
Our second transformation will perform a map/reduce that aggregates Active Food Establishment Licenses and Mayors 24 Hour Hotline reports to determine if there are any restaurants that have had rodent activity nearby. We aggregate on rounded latitude and longitude, to determine the area immediately surrounding a restaurant.
