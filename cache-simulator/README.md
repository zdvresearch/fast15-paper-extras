The Cache Simulator as described in paper: [Analysis of the ECMWF Storage Landscape](https://www.usenix.org/conference/fast15/technical-sessions/presentation/grawinkel)

It is built to use the trace files explained in the paper. They can be downloaded from [here](https://github.com/zdvresearch/fast15-paper-addons)

The environment uses [jq](https://github.com/stedolan/jq) to parse a json based configuration file used in a Makefile.

```
ln -s thirdparty/jq-linux64 jq
```

Generate the configs, run the evaluator, visualize the results:

```
make clean_configs
make gen_config
make run_cache_eval
make visualize
```

