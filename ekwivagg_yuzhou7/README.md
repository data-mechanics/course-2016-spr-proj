<p>Erica Wivagg (ekwivagg@bu.edu)</p>
<p>Yu Zhou (yuzhou7@bu.edu)</p>

<h2>To run:</h2>
<sl>
<li>run_project.py : will run all associated project 1 scripts:</li>
<li>retrieval.py : retrieves and filters all data from City of Boston opendata</li>
<li>rodent_problems.py : performs a join to determine which restaurants have rodent problems nearby</li>
<li>inspections.py : performs a join to determine which licensed restaurants failed inspection</li>
<li>get_crime.py : retrieves all larceny from vehicle crimes</li>
<li>get_closest_stop.py : finds closest stop for every restaurant</li>
<li>get_liquor.py : finds closest stop for every restaurant with a liquor license</li>
</sl>

<h2>Datasets used:</h2>

<h3>Project 1:</h3>
<sl>
<li>Active Food Establishment Licenses</li>
<li>Food Establishment Inspections</li>
<li>Mayors 24 Hour Hotline</li>
</sl>

<h3>Project 2:</h3>
<sl>
<li>Active Food Establishment Licenses</li>
<li>Liquor Licenses</li>
<li>Crime Incident Report</li>
<li>Location of all T stops, pulled from ciestu_sajarvis</li>
</sl>


<h2>Justification:</h2>

<h3>Project 1:</h3>
<p>The question we are looking to answer is: how safe and healthy are restaurants in Boston?</p>
<p>We will look at all licensed restaurants in Boston and find out whether they had inspections, and if so, whether they passed or failed these inspections (combining (1) and (2) above).</p>
<p>Additionally, we will look at the Mayor's Hotline and determine whether restaurants have had problems with rodents or other issues that are reported.</p>
<p>Future project angles may seek to find crime incident reports that occur near restaurants, violations of liquor licenses, or 311 complaints.</p>
<p>The goal is to determine for any particular restaurant what the unsafe aspects or incidents are that have occurred at or nearby the restaurant.</p>
<p>For this assignment, we will focus on determining what inspections have been performed and passed/failed by each restaurant (using (1) and (2) above) and which restaurants have had rodent problems nearby (using (1) and (3) above).</p>

<h3>Project 2:</h3>
<p>The focus of our project has shifted to determining several transit-related metrics about Boston restaurants. Specifically, we will be looking to find the closest T stop to every licensed restaurant and using this information to find out how many restaurants have each T stop as their closest stop. This will serve as a kind of "hotspot" locator for T stops - we will be able to show how many restaurants are closer to a given T stop than to any other T stop. In essence we are establishing a "neighborhood" for each T stop and finding how many restaurants are in that neighborhood.</p>
<p>We will use the T stop neighborhoods again to determine how safe a given area is relative to number of liquor-licensed restaurants in the neighborhood. More precisely, we will determine how many restaurants in a given T stop neighborhood have liquor licenses. We will correlate this number to the number of "larceny from vehicle" crimes that occurred in that neighborhood to determine if neighborhoods with more liquor-licensed restaurants are more likely to have vehicle-related crimes. This will provide a metric to determine if it is safe to drive and park at a restaurant or if you should try to take public transit.</p>
<p>Combined with our Project 1 measurements of restaurant safety based on number of rodent incidents and food inspection failures, the ultimate goal (not realized in this project) is to create a score for each restaurant based on how healthy it is (rats and inspection failures), how safe it is to park there (vehicle crimes), and how far it is from the nearest public transit.</p>

<h2>Transformations:</h2>

<h3>Project 1:</h3>
<p>We will perform a map/reduce that aggregates Active Food Establishment Licenses with Food Establishment Inspections to determine which currently licensed restaurants have failed inspection.
Our second transformation will perform a map/reduce that aggregates Active Food Establishment Licenses and Mayors 24 Hour Hotline reports to determine if there are any restaurants that have had rodent activity nearby. We aggregate on rounded latitude and longitude, to determine the area immediately surrounding a restaurant.</p>

<h3>Project 2:</h3>
<p>Find the closest T stop to every restaurant and to every restaurant with a liquor license. For each T stop find the number of restaurants for which the closest T stop is that stop in order to find the number of restaurants in each T stop's neighborhood."
Correlate the number of vehicle larcenies with the number of restaurants that have a liquor license for every given T stop "neighborhood."</p>

<h2>Data Visualizations (in progress):</h2>
<ul>
<li>Show a bar graph for each T stop of the number of restaurants in that stop's "neighborhood" </li>
<li>Show a correlation between number of restaurants in a given "neighborhood" with a liquor license and number of vehicle larcenies in that "neighborhood".</li>
</ul>
