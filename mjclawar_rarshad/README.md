mjclawar_rarshad
================

Running the package
-------------------
- Create a file named auth.json in the base directory with fields
    - "api_token": A Socrata API token
    - "username": The username with access to the collections
    - "pass": The password associated with the username with access to the collections

- Run `setup.py auth.json`


Data Sets
---------

1. Crime Incident Reports
link: http://https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/7cdf-6fgx
    - 
2. Property Assessment 2015
link: https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
    - 
3. Boston Public Schools (School Year 2012-2013)
link: https://data.cityofboston.gov/dataset/Boston-Public-Schools-School-Year-2012-2013-/e29s-ympv
    - 
4. MBTA Rapid Transit Stops(?)
link: http://www.mass.gov/anf/research-and-tech/it-serv-and-support/application-serv/office-of-geographic-information-massgis/datalayers/mbta.html
    -
5. Hospital Locations
link: https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz
We are interested in evaluating Boston neighborhoods/properties based on a variety of parameters. Important parameters
include incidents of crime and proximity to facilities such as schools and public transportation. We will ideally
include additional relevant data sets such as emergency response times, demographic data, and more. We will come up with
a value metric based on our data. We will then compare our value metric to actual property assessment data and observe
the following:
    - What parameters strongly influence property value?
    - How well does our model agree with actual property data?