from collections import OrderedDict
from time import time


class TimeRecorder(object):
    def __init__(self):
        self.time_logs = OrderedDict()

    def __str__(self):
        out = ''
        for process_name, logs in self.time_logs.items():
            out += process_name + ':\n'
            for k, t in logs.items():
                out += '\t%s:%.20f\n' % (k, t)

        return out

    def register_start(self, process_name):
        self.__register_time(process_name=process_name, key='start')

    def register_end(self, process_name):
        self.__register_time(process_name=process_name, key='end')

    def __register_time(self, process_name, key):
        log = time()
        if process_name not in self.time_logs.keys():
            self.time_logs[process_name] = OrderedDict()

        self.time_logs[process_name][key] = log


