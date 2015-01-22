import subprocess
import glob
import os
import sys
import shutil
import datetime
import threading
import subprocess
import shlex
import json

class Runner(threading.Thread):
    def __init__(self, id, lock, config_dir, wip_dir, results_dir, failed_dir, config_filter):
        threading.Thread.__init__(self)
        self.lock = lock
        self.config_dir = config_dir
        self.wip_dir = wip_dir
        self.results_dir = results_dir
        self.failed_dir = failed_dir
        self.config_filter = config_filter

    def run(self):

        while 1:
            # synchronize on directory access
            with self.lock:
                if config_filter:
                    configs = [x for x in glob.glob(self.config_dir + "/*.json") if os.path.basename(x).__contains__(config_filter)]
                else:
                    configs = glob.glob(self.config_dir + "/*.json")
                if len(configs) == 0:
                    print("finishing work")
                    break
                print("%r configs remaining" % len(configs))

                current_config = configs.pop()

                # create subdir in wipdir. execute everything.
                bn = os.path.basename(current_config)
                wip_dir_name = bn[:bn.find('.')]
                working_dir = os.path.join(self.wip_dir, wip_dir_name)
                os.mkdir(working_dir)
                shutil.move(current_config, os.path.join(working_dir, "config.json"))
                current_config = os.path.join(working_dir, "config.json")

            print ("executing: %s" % (current_config))

            outlog = os.path.join(working_dir, "out.log")
            errlog = os.path.join(working_dir, "err.log")

            # if it fails, move config to failed. otherwise to results.
            cmdline = "python cache_model_evaluation/evaluate_cache_models.py %s %s" % (working_dir, current_config)
            args = shlex.split(cmdline)

            with open(outlog, 'w') as outfile:
                with open (errlog, 'w') as errfile:
                    print("%s executing: %s" % (str(datetime.datetime.now()), cmdline))
                    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=errfile)
                    for line in p.stdout:
                        # print(line)
                        sys.stdout.write(line)
                        sys.stdout.flush()
                        outfile.write(line)
                        outfile.flush()

                    ret = p.wait()
            if ret == 0:
                shutil.move(working_dir, self.results_dir)
            else:
                shutil.move(working_dir, self.failed_dir)


def main(num_processes=None, config_filter=None):

    with open ("evaluation_config.json","r") as config_file:
        config = json.load(config_file)
        print config

    if num_processes:
        processes = num_processes
    else:
        processes = config["parallel_processes"]
    
    config_dir = config["configs_dir"]

    wip_dir = config["wip_dir"]
    if not os.path.exists(wip_dir):
        os.mkdir(wip_dir)

    results_dir = config["results_dir"]
    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    failed_dir = config["failed_dir"]
    if not os.path.exists(failed_dir):
        os.mkdir(failed_dir)

    lock = threading.Lock()
    ps = []
    for i in range(processes):
        p = Runner(i, lock, config_dir, wip_dir, results_dir, failed_dir, config_filter)
        p.start()
        ps.append(p)

    for p in ps:
        p.join()

if __name__ == "__main__":

    if len(sys.argv) == 2:
        num_processes = int(sys.argv[1])
        sys.exit(main(num_processes=num_processes))

    if len(sys.argv) == 3:
        num_processes = int(sys.argv[1])
        config_filter = sys.argv[2]
        sys.exit(main(num_processes=num_processes, config_filter=config_filter))

    sys.exit(main())
