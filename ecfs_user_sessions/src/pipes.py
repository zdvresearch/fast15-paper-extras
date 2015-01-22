from collections import defaultdict
import os

class Pipes():
    def __init__(self, target_dir, suffix="", lines_per_pipe=10000, overwrite=False):
        self.target_dir = target_dir
        self.suffix = suffix
        self.lines_per_pipe = lines_per_pipe
        self.overwrite = overwrite

        self.pipes = defaultdict(list)

    def _flush_pipe(self, name):

        if self.overwrite:
            open_flag = 'w'
        else:
            open_flag = 'a'
            
        with open(os.path.join(self.target_dir, name + self.suffix), open_flag) as tf:
            pipe = self.pipes[name]
            for li in pipe:
                tf.write(li + "\n")
            print("flushed pipe: " + name)
            del self.pipes[name]

    def write_to(self, name, line):
        pipe = self.pipes[name]
        pipe.append(line)
        if len(pipe) >= self.lines_per_pipe:
            self._flush_pipe(name)

    def close(self):
        for name in self.pipes.keys():
            self._flush_pipe(name)   
