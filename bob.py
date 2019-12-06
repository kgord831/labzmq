#!/home/kyle/anaconda3/bin/python
import names
from device import Device

import random
import time
import pickle

if __name__ == "__main__":
    dev = Device(names.BOB)

    def connect():
        dev.logger.info('Bob is starting')
        return dev.start()

    def get_alot():
        dev.logger.info('Bob wants Joe int')
        dev.send([b"JOE", b'GET', b"INT"])
        dev.logger.info('Bob wants to set Joe int')
        dev.send([b"JOE", b'SET', b"INT", pickle.dumps(2)])
        dev.logger.info('Bob wants to confirm the set')
        dev.send([b'JOE', b'GET', b'INT'])
        return 0

    def leave():
        dev.logger.info('Bob is leaving')
        return dev.exit()

    exec_times = [0.2, 0.3, 0.4]
    tasks = [connect, get_alot, leave]
    start_time = time.time()
    for t, task in zip(exec_times, tasks):
        while time.time() - start_time < t:
            dev.loop()
        dev.logger.info('Executing task {}'.format(task))
        if task() < 0:
            break
    dev.logger.info('Bob program is ending')
    dev.exit()
