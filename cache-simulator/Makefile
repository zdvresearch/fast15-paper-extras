clean_all: clean_configs clean_wip clean_results clean_failed
	echo "cleaned all"

clean_configs:
	$(eval CONFIG_DIR := $(shell cat evaluation_config.json | ./jq '.configs_dir'))
	rm -rf ${CONFIG_DIR}

clean_wip:
	$(eval WIP_DIR := $(shell cat evaluation_config.json | ./jq '.wip_dir'))
	rm -rf ${WIP_DIR}

clean_results:
	$(eval RESULTS_DIR := $(shell cat evaluation_config.json | ./jq '.results_dir'))
	rm -rf ${RESULTS_DIR}

clean_failed:
	$(eval FAILED_DIR := $(shell cat evaluation_config.json | ./jq '.failed_dir'))
	rm -rf ${FAILED_DIR}

clean_graphs:
	$(eval GRAPHS_DIR := $(shell cat evaluation_config.json | ./jq '.graphs_dir'))
	rm -rf ${GRAPHS_DIR}

gen_configs:
	$(eval CONFIG_DIR := $(shell cat evaluation_config.json | ./jq '.configs_dir'))
	mkdir -p ${CONFIG_DIR}
	python -m cache_model_evaluation.generate_configs ${CONFIG_DIR}

run_cache_eval:
	$(eval WIP_DIR := $(shell cat evaluation_config.json | ./jq '.wip_dir'))
	mkdir -p ${WIP_DIR}

	$(eval RESULTS_DIR := $(shell cat evaluation_config.json | ./jq '.results_dir'))
	mkdir -p ${RESULTS_DIR}

	$(eval FAILED_DIR := $(shell cat evaluation_config.json | ./jq '.failed_dir'))
	mkdir -p ${FAILED_DIR}

	python run_cache_eval.py $(PROCESSES) $(NAMEFILTER)

visualize:
	$(eval RESULTS_DIR := $(shell cat evaluation_config.json | ./jq '.results_dir'))
	$(eval GRAPHS_DIR := $(shell cat evaluation_config.json | ./jq '.graphs_dir'))
	mkdir -p $(GRAPHS_DIR)
	echo $(GRAPHS_DIR)
	python3 present_results.py ${RESULTS_DIR} /home/shared/meatz/ecmwf_data/results/ecfs/ecfs_trace_hit_ratio_analysis_2012-2014.json $(GRAPHS_DIR)

visualize2:
	$(eval RESULTS_DIR := $(shell cat evaluation_config.json | ./jq '.results_dir'))
	$(eval GRAPHS_DIR := $(shell cat evaluation_config.json | ./jq '.graphs_dir'))
	mkdir -p $(GRAPHS_DIR)
	echo $(GRAPHS_DIR)
	python2 present_results2.py ${RESULTS_DIR} $(GRAPHS_DIR) /home/shared/meatz/ecmwf_data/results/ecfs/ecfs_trace_hit_ratio_analysis_2012-2014.json
