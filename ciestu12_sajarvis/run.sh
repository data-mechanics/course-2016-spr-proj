python3 get_green_line_branch_info.py #gives t_branch_info

python3 get_boarding_info.py #gives green_line_boarding_counts

python3 make_stop_gps_db.py #gives t_stop_locations

python3 make_walk_dist_db.py #gives green_line_walking_distances #needs 1)t_branch_info, 2)t_stop_locations

python3 make_nearest_stops.py #needs green_line_walking_distances

python3 make_people_seconds.py #needs 1)nearest_stops, 2)green_line_boarding_counts

python3 make_normalized_people_seconds.py #needs people_second_utility

python3 make_optimal_stops.py #needs 1)normal_ppl_sec_util, 2)t_stop_locations

python3 make_p_score.py #needs 1)normal_ppl_sec_util, 2)t_stop_locations

python3 generate_plan.py