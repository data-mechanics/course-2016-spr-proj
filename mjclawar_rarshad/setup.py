"""
File: setup.py

Description: Runs the project files with proper data provenance and authentication
Author(s): Raaid Arshad and Michael Clawar

Notes:
"""

import json
import pymongo
import sys
import os

# Make sure the system path is valid for running from command line
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mjclawar_rarshad.reference.dir_info import plan_json
from mjclawar_rarshad.processing import crime_centroids, hospital_distances
from mjclawar_rarshad.sources import crime, property_assessment, boston_public_schools, hospital_locations
from mjclawar_rarshad.tools import bdp_query, database_helpers


def main(auth_json_path, full_provenance=False):
    with open(auth_json_path, 'r') as f:
        auth_json = json.load(f)
        api_token = auth_json['services']['cityofbostondataportal']['token']
        username = auth_json['services']['cityofbostondataportal']['username']
        mongo_pass = auth_json['services']['cityofbostondataportal']['password']

    database_helper = database_helpers.DatabaseHelper(username=username, password=mongo_pass)
    bdp_api = bdp_query.BDPQuery(api_token=api_token)

    if full_provenance:
        with open(plan_json, 'w') as f:
            f.write(json.dumps({}))

    setup_crime_incidents(database_helper, bdp_api, full_provenance=full_provenance)
    setup_property_assessment(database_helper, bdp_api, full_provenance=full_provenance)
    setup_boston_public_schools(database_helper, bdp_api, full_provenance=full_provenance)
    setup_hospital_locations(database_helper, bdp_api, full_provenance=full_provenance)
    setup_crime_centroids(database_helper, full_provenance=full_provenance)
    setup_hospital_distances(database_helper, full_provenance=full_provenance)


def setup_crime_incidents(database_helper, bdp_api, full_provenance=False):
    crime_settings = crime.CrimeSettings()
    crime.CrimeAPIQuery(crime_settings, database_helper, bdp_api).\
        download_update_database(full_provenance=full_provenance)


def setup_property_assessment(database_helper, bdp_api, full_provenance=False):
    property_assessment_settings = property_assessment.PropertyAssessmentSettings()
    property_assessment.PropertyAssessmentAPIQuery(property_assessment_settings, database_helper, bdp_api).\
        download_update_database(full_provenance=full_provenance)


def setup_boston_public_schools(database_helper, bdp_api, full_provenance=False):
    boston_public_schools_settings = boston_public_schools.BostonPublicSchoolsSettings()
    boston_public_schools.BostonPublicSchoolsAPIQuery(boston_public_schools_settings, database_helper, bdp_api).\
        download_update_database(full_provenance=full_provenance)


def setup_hospital_locations(database_helper, bdp_api, full_provenance=False):
    hospital_locations_settings = hospital_locations.HospitalLocationsSettings()
    hospital_locations.HospitalLocationsAPIQuery(hospital_locations_settings, database_helper, bdp_api).\
        download_update_database(full_provenance=full_provenance)


def setup_crime_centroids(database_helper, full_provenance=False):
    crime_centroids_settings = crime_centroids.CrimeCentroidsSettings()
    crime_centroids.CrimeCentroidsProcessor(crime_centroids_settings, database_helper).\
        run_processor(full_provenance=full_provenance)


def setup_hospital_distances(database_helper, full_provenance=False):
    hospital_distances_settings = hospital_distances.HospitalDistancesSettings()
    hospital_distances.HospitalLocationsProcessor(hospital_distances_settings, database_helper).\
        run_processor(full_provenance=full_provenance)


if __name__ == '__main__':
    exec(open('../pymongo_dm.py').read())
    if len(sys.argv) == 1:
        # main('auth.json', full_provenance=True)
        raise ValueError('Please pass in a path to a valid authorization json file meeting the specs in README.md')
    else:
        print(sys.argv)
        main(sys.argv[1])
