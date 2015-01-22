RAW_MARS_FEEDBACK_LOGS_DIR = /home/shared/meatz/ecmwf_data/traces/mars/raw
FILTERED_MARS_FEEDBACK_LOGS_DIR = /home/shared/meatz/ecmwf_data/traces/mars/feedback_filtered/
MARS_RESULTS_DIR = /home/shared/meatz/ecmwf_data/results/mars
MARS_GRAPHS_DIR = /home/shared/meatz/ecmwf_data/results/mars

RAW_MARS_READER_LOGS_DIR = /home/mogonfs/archive_analysis/ecmwf/traces/mars/reader/marsod/users

prepare_feedback_logs_2010:
	python3 feedback/prepare_feedback_logs.py $(RAW_MARS_FEEDBACK_LOGS_DIR) $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2010

prepare_feedback_logs_2011:
	python3 feedback/prepare_feedback_logs.py $(RAW_MARS_FEEDBACK_LOGS_DIR) $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2011

prepare_feedback_logs_2012:
	python3 feedback/prepare_feedback_logs.py $(RAW_MARS_FEEDBACK_LOGS_DIR) $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2012

prepare_feedback_logs_2013:
	python3 feedback/prepare_feedback_logs.py $(RAW_MARS_FEEDBACK_LOGS_DIR) $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2013

prepare_feedback_logs_2014:
	python3 feedback/prepare_feedback_logs.py $(RAW_MARS_FEEDBACK_LOGS_DIR) $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2014

analyze_filtered_feedback_logs_2010:
	pypy feedback/analyze_filtered_feedback_logs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2010

analyze_filtered_feedback_logs_2011:
	pypy feedback/analyze_filtered_feedback_logs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2011

analyze_filtered_feedback_logs_2012:
	pypy feedback/analyze_filtered_feedback_logs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2012

analyze_filtered_feedback_logs_2013:
	pypy feedback/analyze_filtered_feedback_logs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2013

analyze_filtered_feedback_logs_2014:
	pypy feedback/analyze_filtered_feedback_logs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) 8 2014

merge_filtered_feedback_stats:
	python3 feedback/merge_filtered_feedback_stats.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) $(MARS_RESULTS_DIR)/mars_feedback_stats.json

visualize_feedback_stats:
	mkdir -p $(MARS_GRAPHS_DIR)
	python feedback/visualize_feedback_stats.py $(MARS_RESULTS_DIR)/mars_feedback_stats.json $(MARS_GRAPHS_DIR)

visualize_feedback_csvs:
	mkdir -p $(MARS_GRAPHS_DIR)
	python feedback/visualize_feedback_csvs.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR) $(MARS_GRAPHS_DIR)

categorize_mars_retrieve_requests:
	python3 feedback/analyze_requests.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR)

count_feedback_totals:
	python3 feedback/count_feedback_totals.py $(FILTERED_MARS_FEEDBACK_LOGS_DIR)
