PREPARED_TRACES_DIR = /home/shared/meatz/public_html/ecmwf_traces_release/Traces/ECFS_Access_Traces/
USERS_SESSIONS_DATA_DIR = /home/shared/meatz/ecmwf_data/ecfs/user_sessions/
RESULTS_DIR = /home/shared/meatz/ecmwf_data/results/ecfs/user_sessions/
GRAPHS_DIR=/home/shared/meatz/ecmwf_data/ecfs_graphs/user_sessions

prepare_user_sessions:
	mkdir -p $(USERS_SESSIONS_DATA_DIR)
	pypy src/prepare_user_sessions.py $(PREPARED_TRACES_DIR)/ecfs_access_2012.01-2014.05.xz $(USERS_SESSIONS_DATA_DIR)

delete_user_sessions_dir:
	rm -rf $(USERS_SESSIONS_DATA_DIR)

analyze_user_sessions:
	mkdir -p $(RESULTS_DIR)
	pypy src/analyze_user_sessions.py $(USERS_SESSIONS_DATA_DIR) $(RESULTS_DIR)/user_session_results.csv

present_user_sessions:
	mkdir -p $(RESULTS_DIR)
	python src/present_user_sessions.py $(RESULTS_DIR)/user_session_results.csv $(RESULTS_DIR)/user_session_statistics.json

to_latex:
	python src/to_latex.py $(RESULTS_DIR)/user_session_statistics.json

plot:
	mkdir -p ${GRAPHS_DIR}
	#python3 src/plot_cdf_both_per_session.py $(PREPARED_TRACES_DIR)/ecfs_access_2012.01-2014.05.xz $(GRAPHS_DIR)/cdf_both_per_session.pdf
	python3 src/plot_actions_per_session.py $(USERS_SESSIONS_DATA_DIR) $(GRAPHS_DIR)/actions_per_session.pdf

