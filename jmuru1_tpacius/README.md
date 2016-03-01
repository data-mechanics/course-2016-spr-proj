# course-2016-spr-proj-one
Project repository for the first project in the Spring 2016 iteration of the Data Mechanics course at Boston University.

# Project Summary
We chose to work with the Zipcar's datasets on Boston Reversations and Boston Member Counts as well as the City's Property Valuation dataset. We believe that these datasets can be combined together via their Postal Codes to determine if there is a correlation between Zipcar subscriptions numbers, frequency of Zipcar trips in a neighborhood, and value of property in particular neighborhoods. The identification of meaningful intersection between the three datasets could potentially be useful in identifying potential locations for new general purpose parking lots, MTBA stop locations, Zipcar lot expansions and more.

# Python Files
It is assumed that the dbProv script will be ran prior to the ElementaryOperations script to properly create the plan.json file.

Our apitest script uses our own City of Boston Socrata api key to gather raw data to insert in the db. The dbProvOps script uses the apitest script to insert the api query response into the db. The ElementaryOperation script fetches the api results from the db and performs operations that are then inserted into the db.