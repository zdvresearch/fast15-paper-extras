PREPARED_TRACES_DIR = /home/shared/meatz/ecmwf_data/traces/ecfs
RESULTS_DIR = /home/shared/meatz/ecmwf_data/results/ecfs
GRAPHS_DIR  = /home/shared/meatz/ecmwf_data/ecfs_graphs

analyze_all: 
	mkdir -p $(RESULTS_DIR)
	pypy src/analyze_obfuscated_merged_minified.py $(RESULTS_DIR)/ecfs_trace_analysis_2012-2014.json $(PREPARED_TRACES_DIR)/ecfs_logs_2012-2014_obfuscated_sorted.gz

analyze_cache_hit_ratios:
	mkdir -p $(RESULTS_DIR)
	pypy src/analyze_obfuscated_merged_minified_hit_ratios.py $(RESULTS_DIR)/ecfs_trace_hit_ratio_analysis_2012-2014.json $(PREPARED_TRACES_DIR)/ecfs_logs_2012-2014_obfuscated_sorted.gz

visualize_all:
	mkdir -p $(GRAPHS_DIR)
	#python src/visualize.py $(RESULTS_DIR)/ecfs_trace_analysis_2012-2014.json $(GRAPHS_DIR)/
	python src/analyze_NEW_CATEGORIES_SIZE_JSON_DATA.py -i $(RESULTS_DIR)/ecfs_trace_analysis_2012-2014.json -d $(GRAPHS_DIR) 

analyze_user_sessions: analyze_user_sessions_prepare analyze_user_sessions_analyze analyze_user_sessions_to_tex
	echo "done"

analyze_file_access_times:
	mkdir -p $(RESULTS_DIR)/ecfs_file_accesses/
	pypy src/analyze_file_access_times.py $(PREPARED_TRACES_DIR)/ecfs_logs_2012-2014_obfuscated_sorted.gz $(RESULTS_DIR)/ecfs_file_accesses/ecfs_access_times_since_create.lines.txt.gz

visualize_file_retrieve_frequency_cdf:
	mkdir -p $(GRAPHS_DIR)/ecfs_file_accesses/
	python src/visualize_file_retrieve_frequency_cdf.py $(RESULTS_DIR)/ecfs_file_accesses/ecfs_access_times_since_create.lines.txt.gz $(GRAPHS_DIR)/ecfs_file_accesses/

analyze_file_access_counts:
	pypy src/analyze_file_access_count.py $(PREPARED_TRACES_DIR)/ecfs_logs_2012-2014_obfuscated_sorted.gz

present_cache_hit_totals:
	pypy src/present_cache_hit_totals.py /home/shared/meatz/ecmwf_data/results/ecfs/ecfs_trace_hit_ratio_analysis_2012-2014.json