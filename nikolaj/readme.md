**Usage**

The driver script is projone.py. No authentication is required so an auth file is optinal. Run as follows:

```python projone.py [auth.json]```

**Depedencies**

Tested on Ubuntu 12.04 64 bit, Python3.4, Mongo3.2.3.

**Analytics Task**

Determine if there is a gender based pay gap among Boston City Employees. This is the first step to establishing whether there are trends in gender pay gap over time.

**Datasets**

SSA common names by gender: This dataset contains records of first name, gender, and frequency of name. I will use this dataset to infer 
the gender of each employee included in the following datasets. 

https://www.ssa.gov/oact/babynames/names.zip

Employee Earnings Report 2013: This data set contains income data of Boston City employees. Since employee gender is not included, I will use the first name records to cross-reference the previous data set to establish gender. 

https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2013/54s2-yxpg

Employee Earnings Report 2014: Same as the previous dataset but for the year 2014.

https://data.cityofboston.gov/Finance/Employee-Earnings-Report-2014/4swk-wcg8

**Transformations**

Aggregate gender records with salary information: I provide a MongoDB mapreduce implementation for combining employee income records from Employee Earnings Report 2013, and Employee Earnings Report 2014 with the gender name information provided in SSA common names by gender to establish the gender of the employees.
This creates two new data sets of individual employee gender to salary mappings for the years of 2013 and 2014. 

Average income by gender: I provide a MongoDB mapreduce implementation for computing the average income by gender using the two data sets generated in the previous transformation.
