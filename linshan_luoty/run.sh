#python3 retrieve_datasets.py ../auth.json 

#python3 extract_zip_info.py ../auth.json  #needs retrieve_datasets & approved_building_permits #gives zip_locations

#python3 update_crime_lng_lat.py ../auth.json #needs crime_incident_reports

#python3 extract_earnings_zips.py ../auth.json  #gives earning_zips

#python3 get_avg_earning.py ../auth.json #needs earning_zips  #gives zip_avg_earnings

#python3 merge_crime_with_zip.py ../auth.json #gives crime_zips #needs crime_incident_reports & zip_locations

#python3 get_total_crimes.py ../auth.json  #gives zip_location_crimes #needs crime_zips

#python3 merge_zip_crime_earnings.py ../auth.json #gives zip_location_crimes_earnings  #needs zip_location_crimes & zip_avg_earnings 




