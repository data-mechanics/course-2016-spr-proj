"""
Runs the project files with proper data provenance and authentication

Authors: Michael Clawar and Raaid Arshad
2/13/2016
"""

from mjclawar_rarshad import setup_provenance
from mjclawar_rarshad.api import crime
import pymongo


def setup_crime_incidents():
    crime_settings = crime.CrimeSettings()
    crime.CrimeAPIQuery(crime_settings).download_update_database()

if __name__ == '__main__':
    exec(open('../pymongo_dm.py').read())
    setup_provenance.initialize_provenance()
    setup_crime_incidents()
