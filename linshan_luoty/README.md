BU CS591 Data Mechanics Project two
===================================

Team members: 
+ Linshan Jiang
+ Tianyou Luo
  
Abstract
--------
In the project, we are interested in the relationship between the average income and the number of crime incidents. 
We use zipcodes as grouping units to analyze.

The three datasets we used are:
<ol>
<li>'crime_incident_reports':'https://data.cityofboston.gov/resource/7cdf-6fgx.json?year=2014'</li>
<li>'employee_earnings_report_2014':'https://data.cityofboston.gov/resource/4swk-wcg8.json'</li>
<li>'approved_building_permits':'https://data.cityofboston.gov/resource/msk6-43c6.json</li>
</ol>

From the first dataset we can obtain crime reports with specific locations (longitude and latitude). 
From the second dataset we know the link between zipcode and earning.
From the third dataset we can get the relatinoship between location and zipcodes. 
Combine(union) the first and third dataset by location, aggregate by (zipcode,sum, K mean of locations) we can get the number of crimes in each zipcode area, and also the location of each zipcode!
Combine(union) the second and third dataset by location, aggregate by (zipcode, avg) we can get average income in each zipcode area.
Then combine the result datasets by zipcode, we can get the relationship between average earning and number of crimes of each zipcode. 

We can draw distribution of crimes in each zipcode location on "map" and show where in Massachussets subjects to more crimes.
(location_crimes.html)
We also draw distribution of average income in each zipcode location on "map". Thus we can see overall, people in which area earns more or less, and whether they encounter more criminals :)
(location_earnings.html)
Lastly, we draw a diagram using the third resulting dataset and see the relation between income level and number of crimes.
(income_crimes.html)

Note:
To see the graphs, please open the three htmls in FIRE FOX... fire fo.. fire f.. fire .. fir.. fi.. f..
