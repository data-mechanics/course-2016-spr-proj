Optimizing Green Line T Stops
=============================

The Green Line seems to stop too frequently. Presuming the best location for a stop is where one doesn't already exist and people would most use one, we think analyzing the popularity of stops against the distance to alternatives can help decide which existing stops are most valuable and which can be omitted. To make such a determination, we plan to use a variant of the k-means clustering algorithm that also takes into account a given weight for each point. The weight, in this case, will be some function of the stop's popularity and the proximity to the nearest alternative.

By using *k-means* and setting *k* to be *x* fewer than the number of existing stops, we can determine which *x* stops are best to remove from existing routes: the *k* stops nearest the cluster means (and only one stop per mean) would be the ones to persist.

# The Data Sets Involved
TODO add descriptions for each of these

### 1. GPS locations of T stops

### 2. Line associations of T stops

### 3. Popularity of each Green Line stop

### 4. (derived) Walking distances to other stops on a branch

### 5. (derived) ??
