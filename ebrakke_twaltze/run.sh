#python3 work_zones.py ../auth.json #gives workZones
#python3 construction_filter.py ../auth.json #gives construction #requires workZones

python3 pothole_filter.py ../auth.json #gives potholes  #map reduce failed
####python3 crime_reports.py ../auth.json #gives crimeReports  #line 55: name 'get_crime_reorts' is not defined

#python3 accident_filter.py ../auth.json #gives accidents #requires crimeReports

#python3 calculate_danger_levels.py ../auth.json #gives dangerLevels  #requires pothole & accident & constructions

#python3 get_incidents_along_route.py ../auth.json #requires dangerLevel  #line20: list index out of range

#python3 cluster_danger_zones.py ../auth.json #requires dangerLevel


#python3 get_random_routes.py ../auth.json #requires dangerLevels #gives randomDirections
#python3 average_incidents_along_path.py ../auth.json #gives randomDirectionIncidentCount #requires dangerLevels & randomDirections

#python3 calculate_average_per_meter.py ../auth.json #requires random DirectionIncidentCount

#python3 distance_test.py ../auth.json

