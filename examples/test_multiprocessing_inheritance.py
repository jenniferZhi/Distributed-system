# test_multiprocessing_inheritance.py
# Andrew DeOrio
# 2016-09-20
#
# First, start this program, which starts 5 threads
# $ python3 test_multiprocessing.py
#
# Then, view the processes and threads running on your system
# $ ps -ax
#
# Reference: https://docs.python.org/3/library/multiprocessing.html

import os
from multiprocessing import Process

class Worker(Process):
    def __init__(self, worker_id):
        # Call parent constructor
        super(self.__class__, self).__init__()

        # Save inputs
        self.id = worker_id

        pid=os.getpid()
        print("created worker {} pid={}".format(self.id, pid))

    def run(self):
        """Override Process's run function"""
        print("worker {} run()".format(self.id))
        while True:
            pass
        

if __name__ == "__main__":
    print "hello world"
    processes=[]
    for i in range(5):
        p = Worker(i)
        processes.append(p)
        p.start()
    p.join()
