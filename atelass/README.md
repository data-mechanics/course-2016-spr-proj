## Datasets
The three datasets being used in this project, all from the city of Boston, are Crime Incident Reports (2012-2015), Streetlight Locations, and MBTA T Stops by Location. The idea behind combining these datasets is to characterize the relative safety of different T stops at night. Ideally, one would assume that the more streetlights there are in a given location, the less likely a crime would occur. As such, when we characterize the safety of these stops at night, we can examine our results to see if this assumption is true or not. Note on credentials: only an API key for the MBTA data is necessary, as I was able to collect the other data without authentication.

## Scripts
Descriptions for what each of the scripts does can be found in the comments at the top of each script.
The scripts should be run in Python 3 (with the exception of mapping.py) in the following order:

1. `data_retrieval.py`: This should run in approximately 3 minutes. Note: This requires an `auth.json` file with a valid API key for the MBTA data.

2. `data_manipulation.py`: This should run in approximately 10 minutes.

3. `correlation.py`: This should run in a few seconds.

4. `scoring.py`: This should run in approximately 5 minutes.

5. `mapping.py`: This should run in approximately 5 minutes. IMPORTANT: This script must be run in Python 2.

## Analysis of Data
As stated before, the goal of this project was to characterize the relative safety of the T stops at night and check to see if a correlation exists between the number of streetlights and the number of crimes for a given station. In order to find out if a correlation exists, I calculated the Pearson correlation coefficient.

Going into this project, I assumed my data would show a negative correlation; however, after actually calculating the correlation coefficient, there appeared to be a slight positive correlation. This makes sense, though, considering there are more important factors that determine crime rates than just the number of streetlights, and a higher number of streetlights can be indicative of some of these factors (e.g., population sizes).

Next, I planned to solve a constraint satisfaction problem, wherein I would decide where best to place a given number of streetlights to reduce the overall number of crimes. However, given that the correlation turned out to be positive, it didn't seem like this constraint satisfaction problem would be useful (at least based on the results of this data). That being said, I instead decided to assign scores for each of the stations, and thus have a more concrete characterization of the relative safety of each of these stations, rather than just using the raw number of crimes as the measure of safety.

My metric for scoring was a weighted sum. I got all crimes between the hours of 9PM and 1AM (excluding 1AM) and for each station counted the number of crimes between 9PM and 9:59PM, 10PM and 10:59PM, 11PM and 11:59PM, and 12AM and 12:59AM. I then calculated the weighted sum of these crimes, using the following weights: 4 for crimes between 9PM and 9:59PM, 3 for crimes between 10PM and 10:59PM, 2 for crimes between 11PM and 11:59PM, and 1 for crimes between 12AM and 12:59AM.

My reasoning for these weights was the assumption that crimes would be more likely as it gets later and the stations become less crowded (although this may depend on the type of crime), so the earlier crimes were weighted more to show that they have more of an impact on the overall score.

## Visualizations
`crimes_and_streetlights.html` is a map showing all the crimes (in red) and streetlights (in yellow). You can zoom in/out on this map and move around.

`stations_and_scores.html` is a zoomable circle packing visualization. It groups stations into circles by scores (in ranges such as 0-99, 100-199, etc...). Clicking on one of these circles allows you to see the individual stations and their scores for the given range. These values are stored in `scores.json`.
