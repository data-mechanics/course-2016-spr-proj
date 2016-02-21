"""
Runs the project files with proper data provenance and authentication

Authors: Michael Clawar and Raaid Arshad
2/13/2016
"""

import json
import pymongo

from mjclawar_rarshad.sources import crime, property_assessment, boston_public_schools, hospital_locations
from mjclawar_rarshad.tools import bdp_query, database_helpers
from mjclawar_rarshad.processing import crime_centroids


def main(auth_json_path):
    # TODO put argv for auth.json here
    with open(auth_json_path, 'r') as f:
        auth_json = json.load(f)
        api_token = auth_json['api_token']
        username = auth_json['username']
        mongo_pass = auth_json['pass']

    database_helper = database_helpers.DatabaseHelper(username=username, password=mongo_pass)
    bdp_api = bdp_query.BDPQuery(api_token=api_token)

    # setup_crime_incidents(database_helper, bdp_api)
    # setup_property_assessment(database_helper, bdp_api)
    # setup_boston_public_schools(database_helper, bdp_api)
    # setup_hospital_locations(database_helper, bdp_api)
    crime_centroids.kmeans_crime(database_helper)


def setup_crime_incidents(database_helper, bdp_api):
    crime_settings = crime.CrimeSettings()
    crime.CrimeAPIQuery(crime_settings, database_helper, bdp_api).download_update_database()


def setup_property_assessment(database_helper, bdp_api):
    property_assessment_settings = property_assessment.PropertyAssessmentSettings()
    property_assessment.PropertyAssessmentAPIQuery(property_assessment_settings,
                                                   database_helper,
                                                   bdp_api).download_update_database()


def setup_boston_public_schools(database_helper, bdp_api):
    boston_public_schools_settings = boston_public_schools.BostonPublicSchoolsSettings()
    boston_public_schools.BostonPublicSchoolsAPIQuery(boston_public_schools_settings,
                                                      database_helper,
                                                      bdp_api).download_update_database()


def setup_hospital_locations(database_helper, bdp_api):
    hospital_locations_settings = hospital_locations.HospitalLocationsSettings()
    hospital_locations.HospitalLocationsAPIQuery(hospital_locations_settings,
                                                 database_helper,
                                                 bdp_api).download_update_database()


if __name__ == '__main__':
    exec(open('../pymongo_dm.py').read())
    main('auth.json')
