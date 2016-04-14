#CS591L1: Data Mechanics Project Two

#Introduction
In this project, we sought to test hypotheses about relationships between hospitals, traffic jams, and property values. More specifically, we wanted to determine:
	<ol>
	 <li> if traffic jams were more likely in zip codes with lower property values and </li>
	 <li> if hospitals are more prevalent in areas with higher property values </li>
	</ol>

#Narrative and Justification
To test these hypotheses, we reduced several datasets relating to local hospitals, traffic delay, and property value assessments. We focused on finding streets prone to traffic delay that hospitals were on, the number of hospitals in particular zip codes, and the average property values in different zip codes. More specifically, we made reductions on: 
	<ol>
	<li> Waze Street Jam data and Boston Hospital locations using street intersections as an operator to determine how frequently traffic jams occurred on streets hospitals were located on in a window of time between February 2015 and March 2015. </li>
	<li> Property Assessments data and Hospital locations using zip codes as an operator to determine how many hospitals are each each zip code </li>
	<li> Property Assessments data and Hospital locations using zip codes as an operator to determine the average property value of each Boston zip code </li>
	</ol>
With these reductions, we believe that we should be able to determine if relationships between hospital locations and traffic patterns as well as hospital locations and hospital location and property values are both present and significant.

#Findings and Conclusions
After reducing the initial datasets to a fine enough granularity to answer these questions, we found that the both of our questions had some merit based on our data. Primarily, that there appears to be a very weak negative correlation (-0.00119) in average property value and the number of hospital in a zip code as well as a moderate positive correlation (0.67221) between number of traffic jams near hospitals in a zip code and the average property value. The calculated p-values (1.0 and 0.055 respectively) for calculated correlations suggest that in terms of "standard" p-value interpretation that both of these findings are significant. Furthermore, despite taking a sample of 10 percent of the property assessment dataset, we believe that the lack of consistent random samples from that pool and the limited window of traffic we were able to gather dampens some strength of these claims. 

#Visualizations
We extracted the relevant collections into json files using the dataExtract.py script. The notable derived and visualized datasets in the database are in the collections avg\_property\_hospital\_count, avg\_property\_trafficjams and hospital\_jams\_count and can be viewed in the files counterVal.html, jamValue.html, index.html

#Replication
##Reductions and Database Operations
In order to recreate our results, the followings scripts should be run in the following order:
	<ol>
	<li>dbOperations.py</li>
	<li>elementaryOperations.py</li>
	<li>databaseIntersectionOp.py</li>
	<li>stats.py</li>
	</ol>
The notable final derived datasets from which we calculated correlation coefficients and p-values in the database are avg\_property\_hospital\_count, avg\_property\_trafficjams

