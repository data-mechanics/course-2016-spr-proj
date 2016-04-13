# Neighborhood Classification

by Daren McCulley and Jasper Burns

## Execution

To run, start the MongoDB server and run project.py from the command line, which does nothing more than connect to the database and execute the following scripts in succession:
   - reset.py
   - get_apartments.py
   - collapse_apartments.py
   - clean_apartments.py
   - get_assessments.py
   - clean_assessments.py
   - join.py
   - compute_ratio.py
   - comp_vit.py

To view the visualization for Assessment vs Rent, open djmcc_jasper/part1/index.html. 
To view the visualization for Composition vs Vitality, open djmcc_jasper/part2/index.html

Datasets in JSON format are generated at the end of each execution. We uploaded these datasets under the same names to https://datamechanics.io/djmcc_jasper/. This is the url referenced by the two main.js Javascript files supporting each visualization.

###### Notes

Running comp_vit.py requries access to the Yelp API and the installation of the rauth python module. The prov python module, for generating provenance documents, is the only other non-standard requirement. Generating the data.json file used for the visualization in Part 1 requires running generateJSON.py on the database following a successful run of project.py. This script is located in djmcc_jasper/tools directory.

## Abstract

In this project, our aim was to learn more about Boston neighborhoods (or more functionally, zip codes) in relation to each other and the city as a whole. We did so in two ways:

#### Part I - Assessed Value vs Rent

In an effort to characterize Boston neighborhoods we decided to take a closer look at the available data on the rental market and combine it with the property assessment data made public by the city. The city’s assessing website describes three approaches to determining an assessed value. Our focus is on the Income Approach, which employs data to determine what a property might earn. One of our main objectives is to reduce the uncertainty inherent in the word might. In a city like Boston, where nearly 40% of all households rent and rental vacancy has decreased year over year for the past decade towards 3%, the data necessary to support the Income Approach is widely available.

The code scrapes real time rental market data from PadMapper in the form of markers. Markers are aggregated on location, preserving the mean rent, under the assumption that two listings with the exact same latitude and longitude have the same address. Each marker is passed back to PadMapper to obtain a string containing address information. Assessments are also aggregated to produce an assessed value per unit for each strictly residential property. Finally assessment data and rental data are joined on the address attribute to produce the final data set.

###### Data Resources

Boston's Property Assessment 2015
```
https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
```
PadMapper
```
https://www.padmapper.com/
```

#### Part II - Composition vs Vitality

The second thing we did draws inspiration from [an existing study](https://www.technologyreview.com/s/601107/data-mining-reveals-the-four-urban-conditions-that-create-vibrant-city-life/#/set/id/601103/), which examined the relationship between the vitality of neighborhoods compared to the diversity of their composition (in terms of commercial or residential zoning). So we did the same: by analyzing Boston’s 2015 Property Assessment Data, we were able to assign each zipcode a diversity score, which we said was equal to the percent commercial of a neighborhood (Commercial Units / (Commercial Units + Residential Units)).

We then had to define a vitality score for the y-axis. We define the vitality as the average reviews per business for the top ranked 300 businesses in that zipcode in Yelp’s API. The thinking here was that vitality implies a lot of visitation to local businesses, and the more visitations implies more people writing reviews. By making it an average *per business*, this helps normalize for the fact that zipcodes with greater percent commercial would have had more total reviews. Future development, if given greater access to more revealing datasets, could simply rewrite the vitality function in comp_vit.py to further qualify what it means to be of "high vitality."

###### Data Resources

Boston's Property Assessment 2015
```
https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
```
Yelp Search API
```
https://www.yelp.com/developers/documentation/v2/search_api
```

## Credits

The visualizations for this project are built upon preexisting code by [Peter Cook](http://animateddata.co.uk) from his project [What makes us happy?](http://charts.animateddata.co.uk/whatmakesushappy/).