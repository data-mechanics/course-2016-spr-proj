mjclawar_rarshad
================


Dependencies
------------
- See `requirements.txt`
    - To install pandas on Windows, use the .whl file found here: http://www.lfd.uci.edu/~gohlke/pythonlibs/#pandas
    - To install scikit-learn on Windows, use the .whl file found here: http://www.lfd.uci.edu/~gohlke/pythonlibs/#scikit-learn
    - scikit-learn requires numpy+mkl, found here: http://www.lfd.uci.edu/~gohlke/pythonlibs/#numpy


Running the package
-------------------
- Create a file named auth.json with fields

```json
{
    "services": {
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": username,
            "password": password,
            "token": token (API token for the City of Boston portal)
        }
    }
}           
```

- Run `setup.py auth.json`


Reference information
---------------------
The Provenance document for our project can be found in `reference/plan.json`
A depiction of the network graph is provided in the svg `reference/provenance.svg`


Data Sets
---------

1. Crime Incident Reports
link: http://https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/7cdf-6fgx

2. Property Assessment 2015
link: https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q

3. Hospital Locations
link: https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

4. Boston Public Schools (School Year 2012-2013)
link: https://data.cityofboston.gov/dataset/Boston-Public-Schools-School-Year-2012-2013-/e29s-ympv


We are interested in evaluating Boston neighborhoods/properties based on a variety of parameters. Important parameters
include incidents of crime and proximity to facilities such as schools and public transportation. We will ideally
include additional relevant data sets such as emergency response times, demographic data, and more. We will come up with
a value metric based on our data. We will then compare our value metric to actual property assessment data and observe
the following:

    - What parameters strongly influence property value?
    - How well does our model agree with actual property data?

In a previous version of this project, we began building the foundation for neighborhood evaluation. In particular,
we found "crime centroids",
about which clusters of crime tend to have occurred in 2015: `processing/crime_centroids.py`
- The "optimal" number of crime centers is estimated using a naive rule of a 1% change in the Bayesian Information Criterion (BIC)

We also estimated the distance between properties in Boston and the nearest hospital using a spherical approximation:
 `processing/hospital_distances.py`

In this iteration, we used k-nearest neighbors regression with latitude and longitude as the design matrix for
predicting smoothed housing values per square foot across Boston.

We also extend the crime centroids analysis by again using k-nearest neighbors regression
on latitude and longitude to predict whether crimes at a given location are
more likely to occur on weekends (Fridays to Sunday) or on weekdays (Monday to Thursday).

We plot both the housing values and crime classifications using Leaflet
and OpenStreetMap and CartoDB map layers with an extended version of the excellent `mplleaflet` package.
To view the home values data, run the package using `setup.py` and open `home_value_model.html` in a browser.
Similarly for the crime predictions, open `crime_knn_weekday.html` in a web browser.
