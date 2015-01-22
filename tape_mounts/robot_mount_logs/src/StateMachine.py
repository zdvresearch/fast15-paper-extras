__author__ = 'maesker'
import sys

class FiniteStateMachine:
    def __init__(self):
        self.transitions={}
        self.state = None
        self.defaultErrorState = 0
        self.last_event_ts = 0
        self.subsequent_events = []

    def add_transition(self, event, src, dest, callback, rules={}):
        if callback==None:
            callback = self.dummy_callback
        if src not in self.transitions.keys():
            self.transitions[src] = []
        for t in self.transitions[src]:
            if t[0] == dest:
                print "Error transition already exists"
                return None
        self.transitions[src].append((event, dest, callback, rules))

    def get_transition(self, event, arguments):
        resdest=None
        rescb=None
        s = self.transitions.get(self.state, None)
        if s == None:
            msg = "invalid state:%s, no transition, event:%s, arguments:%s"%(self.state, event,str(arguments))
            raise BaseException(msg)
        else:
            candidates = []
            for (evnt, dst, cb, rules) in s:
                if event==evnt:
                    candidates.append((evnt, dst, cb, rules))
            if len(candidates)==1:
                rescb = candidates[0][2]
                resdest =  candidates[0][1]
            elif len(candidates)>1:
                # events need rules
                for can in candidates:
                    to = can[3].get('timeout', None)
                    if to != None:
                        if arguments['epoch'] - self.last_event_ts > to:
                            resdest=can[1]
                            rescb = can[2]
                            break
        return (resdest,rescb)

    def handle_event(self, event, arguments={}):
        rr = None
        (dst, cb) = self.get_transition(event, arguments)
        if dst == None:
            raise BaseException("no transition found, state:%s, evnt,%s, args:%s"%(self.state,event,str(arguments)))
        else:
            (ret, rr) = cb(event, arguments)
            if ret:
                self.state=dst
                self.last_event_ts = arguments['epoch']
            #else:
            #    raise "callback returned %s, no state change"%ret
        if len(self.subsequent_events) <= 0:
            return rr
        else:
            x = self.subsequent_events.pop(0)
            return self.handle_event(x[0], x[1])

    def dummy_callback(self,event, args):
        return (True, None)