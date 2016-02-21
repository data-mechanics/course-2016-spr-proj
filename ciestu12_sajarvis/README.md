Optimizing Green Line T Stops
=============================

The Green Line seems to stop too frequently. Presuming the best location for a stop is where one doesn't already exist and people would most use one, we think analyzing the popularity of stops against the distance to alternatives can help decide which existing stops are most valuable and which can be omitted. To make such a determination, we plan to use a variant of the k-means clustering algorithm that also takes into account a given weight for each point. The weight, in this case, will be some function of the stop's popularity and the proximity to the nearest alternative.

By using *k-means* and setting *k* to be *x* fewer than the number of existing stops, we can determine which *x* stops are best to remove from existing routes: the *k* stops nearest the cluster means (and only one stop per mean) would be the ones to persist.

Here are the data sets we have to help solve this problem, along with why we believe they're helpful.

# The Data Sets Involved

### 1. GPS locations of T stops
This dataset uses information provided by the MBTA, which gives the exact GPS locations of each T-stop on the Green Line. This is used to help us create the dervied datasets. 
This is hosted at http://cs-people.bu.edu/sajarvis/datamech/mbta_gtfs/

### 2. Line associations of T stops
This dataset includes the stop-id, stop name, line, next inbound stop, and next outbound stop. We use this to get a sense of how each branch is ordered, and again to create the derived datasets. This information was provided by the MBTA. 
This is hosted at http://cs-people.bu.edu/sajarvis/datamech/

### 3. Popularity of each Green Line stop
This dataset provides to average boarding population per day at each stop along the green line. We will assume that the number of boardings is proportional to the number of passengers that disembark. This information was provided by the MBTA, using the most recent boarding counts (2013).  
This is hosted at http://cs-people.bu.edu/sajarvis/datamech/

### 4. (derived) Walking distances to other stops on a branch
This derived dataset uses Google API to get the walking distances to the next nearest stop. This looks at the walking distances from the passengers current T-stop position to all other T-stop positions within that branch. 
NOTE: This will take a *long* time to load because Google's API throttles the number of requests we can make. 

### 5. (derived) Time to nearest neighbor stop on same branch
This derived dataset allows us to put a weight on each stop based on the time it takes to get there from the previous stop. If the two stops are really close together, then that means that the current stop is less important and could potentitally be removed. 

### 6. (derived) Utility measurement based on passenger saved time
In this derived dataset, we create a measurement called "people-seconds." This is a score that uses both popularity and walking time to the next nearest stop, and provides a weigth for each stop. A low score is good; this means that the stop is valued. This score will let us perform a weighted *k*-means to find the optimal *k* stops. 








