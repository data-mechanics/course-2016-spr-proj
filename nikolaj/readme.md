NOTE: This is an in-progress commit. 

NOTE: Currently the code will not run with authentication on due to an access issue for creating indexes on a collection.
Will continue working to hopefully resolve the problem.

Usage:

Run "python pagerank_pipeline.py" to step through the data generation process.

Open index.html to view visualizations. NOTE: index.html accesses datamechanics.io.

Dependencies:

Python Beautiful Soup: https://www.crummy.com/software/BeautifulSoup/

Pip install: pip install beautifulsoup4

I used this library to scrape mbtainfo.com for bus schedules.

TODO:

Fix authorization issue when creating indeces on collection.

Clean up in general but especially index.html and pagerank_pipeline.py

Go over provenance

Add convergence check to pagerank code

To actual write-up
