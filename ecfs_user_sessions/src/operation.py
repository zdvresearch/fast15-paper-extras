class Operation:
    def __init__(self):
        self.optype = None  # 'g'(et), 'p'(ut), 'd'(el)
        self.ts = 0
        self.obj_id = None
        self.parent_dir_id = None
        self.size = 0
        self.execution_time = 0

    def __str__(self):
        return ("%s;%d;%s;%s;%d;%d" % (
            self.optype,
            self.ts,
            self.obj_id,
            self.parent_dir_id,
            self.size,
            self.execution_time
            ))

    def init(self, string):
        s = string.split(";")
        self.optype = s[0]
        self.ts = int(s[1])
        self.obj_id = s[2]
        self.parent_dir_id = s[3]
        self.size = int(s[4])
        self.execution_time = int(s[5])


