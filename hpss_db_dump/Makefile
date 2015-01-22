ECFS_TRACE_DIR = /home/shared/meatz/ecmwf_data/traces/ecfs/
ECFS_RESULTS_DIR = /home/shared/meatz/ecmwf_data/results/ecfs/db_dump

MARS_TRACE_DIR = /home/shared/meatz/ecmwf_data/traces/mars/
MARS_RESULTS_DIR = /home/shared/meatz/ecmwf_data/results/mars/db_dump


analyze_ecfs: 
	mkdir -p $(ECFS_RESULTS_DIR)
	python src/analyze.py $(ECFS_TRACE_DIR)/ecfs_db_dump_20140905.gz $(ECFS_RESULTS_DIR)/ ECFS

count_ecmwf_group_files:
	pypy src/count_ecmwf_group_files.py $(ECFS_TRACE_DIR)/ecfs_db_dump_20140905.gz

analyze_mars: 
	mkdir -p $(MARS_RESULTS_DIR)
	python src/analyze.py $(MARS_TRACE_DIR)/mars_db_dump_20140904.gz $(MARS_RESULTS_DIR)/ MARS
