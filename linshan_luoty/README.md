BU CS591 Data Mechanics Project One
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

From the first dataset we can obtain crime reports with specific locations. From the second dataset we can get the relatinoship
between location (in longtitude and latitude) and zipcodes. From the third dataset we know the link between zipcode and earning.
we first combine then first and second dataset by location, then combine the result dataset with the third one by zipcode.

After we get the result dataset, we can analyze the relationship between average earning and crime rate.

Note:
+ We havn't implement the provided 'auth.json' format yet, instead, our customized auth file is formated as 'sample_auth.json'.
