RAW_TRACES_DIR = /Volumes/snail/ECMWF_logs/ROBOT
PREPARED_TRACES_DIR = /Volumes/snail/ECMWF_logs/ROBOT_prepared
RESULTS_DIR = /Volumes/snail/ECMWF_wip/robot

prepare: sanitize_merge insert_broken check_log_file_sanity
	echo "running"

sanitize_merge:
	python src/merge_and_sanitize.py sanitize_merge $(RAW_TRACES_DIR) $(PREPARED_TRACES_DIR) /tmp/broken_lines

insert_broken:
	python src/merge_and_sanitize.py insert_broken /tmp/broken_lines $(PREPARED_TRACES_DIR)

check_logs:
	python src/merge_and_sanitize.py check_log_file_sanity $(PREPARED_TRACES_DIR)


aggregate:
	python src/analyze_logs.py aggregate $(PREPARED_TRACES_DIR) $(RESULTS_DIR)

analyze:
	python src/analyze_logs.py analyze $(RESULTS_DIR)

present:
	python src/analyze_logs.py present $(RESULTS_DIR)