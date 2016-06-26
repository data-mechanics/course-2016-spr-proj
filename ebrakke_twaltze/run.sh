python3 311_requests.py ../auth.json

python3 work_zones.py  #gives workZones

python3 construction_filter.py  #gives construction #requires workZones

python3 pothole_filter.py  #gives potholes

python3 crime_reports.py  #gives crimeReports

python3 accident_filter.py  #gives accidents #requires crimeReports

python3 calculate_danger_levels.py ../auth.json #gives dangerLevels  #requires pothole & accident & constructions

python3 cluster_danger_zones.py ../auth.json #requires dangerLevel

python3 get_random_routes.py  #requires dangerLevels #gives randomDirections

python3 average_incidents_along_path.py  #gives randomDirectionIncidentCount #requires dangerLevels & randomDirections

python3 calculate_average_per_meter.py  #requires random DirectionIncidentCount

python3 distance_test.py ../auth.json

#python3 get_incidents_along_route.py  #requires dangerLevel  #line20: list index out of range
