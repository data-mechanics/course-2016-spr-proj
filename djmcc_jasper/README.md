# Neighborhood Classification

by Daren McCulley and Jasper Burns

## Runnning

To run, execute project.py.

## Notes

Three data resources are pulled from Boston’s Datasets, however we had to handtype a json file in order to understand what property codes stored in each of Property Assessments 2014 and Property Assessments 2015 were. This file, ptype.json, is based off of the following website:
```
https://www.cityofboston.gov/images_documents/MA_OCCcodes_tcm3-16189.pdf
```

## Abstract

We decided to tackle the problem of characterizing neighborhoods for economic condition and gentrification, one of the issues raised in discussions with the City of Boston.For this first project, we started by looking at what we could glean from different property data. We pulled from three City of Boston data sets (1) Property Assessments 2014 (2) Property Assessments 2015 and (3) Approved Building Permits. Additionally, we hand-typed a fourth resource: a json file that translates building codes to building uses which we use to help our computations. These sets together allow us to categorize neighborhoods by type of buildings (Residential, Commercial, Industrial, etc), how these types changed year to year, and where building permits were being applied for to determine the trajectory of these neighborhood’s property demographics.We created three seperate data sets. The first, neighborhood_zoning, aggregates the data in Property Assessments 2014 and Property Assessments 2015 to provide us with counts of buildings of each usetype (Multiuse, Residential, Apartment Commercial, Industrial, Exempt) for every zipcode for each year, and additionally computes the percentage of that zipcode that each usetype takes up. This data is then outputted into neighborhood_zoning as a collection of documents, one for each zipcode with the counts and percents stored inside.The second and third dataset are related; they run a k-means algorithm on the Approved Building Permits dataset to see in which areas of each zipcode the most construction is occuring, and stores in two seperate collections, one for residential and one for commercial. For each parent zip code there is at most three documents in each residential_centers and commercial_centers collections. These documents store the means for each usetype as lat,long as well as street address (which it pulls from geonames.com).As we move forward into future projects, this data will help us categorize neighborhoods using each zip code’s current properties as well as potential trends given how the usetypes for these properties change over time.

## Boston Data Resources

Property Assessment 2014
```
https://data.cityofboston.gov/dataset/Property-Assessment-2014/qz7u-kb7x
```
Property Assessment 2015
```
https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
```
Approved Building Permits
```
https://data.cityofboston.gov/Permitting/Approved-Building-Permits/msk6-43c6
```