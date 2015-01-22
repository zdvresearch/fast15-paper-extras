The MARS logs are based on two raw sources:
	- Feedback logs that are gathered on the requesting clients. They contain the queries and the size of the results (bytes / fields).
	- Reader logs that contain all information about what has happened on the backend movers.


make prepare_reader_logs_20** obfuscates the feedback logs and filters relevant fields
make analyze_filtered_reader_logs_20** uses the daily filtered logs and creates stats.json files for every day
make merge_filtered_reader_stats merges all theses logs into one big json file
	creates: mars_feedback_stats.json

visualize_feedback_stats creates visualizations:
	users: mars_feedback_stats.json
	creates: mars

