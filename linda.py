#!/home/kyle/anaconda3/bin/python
import names
from device import Device

import random
import time

if __name__ == "__main__":
    dev = Device(names.LINDA, INT=6)

    def connect():
        dev.logger.info('Linda is starting')
        return dev.start()

    def get_int():
        dev.logger.info('Linda wants an int')
        dev.send([b"JOE", b"GET", b"INT"])
        return 0

    def get_float():
        dev.logger.info('Linda wants a float')
        dev.send([b"JOE", b"GET", b"FLOAT"])
        return 0

    def leave():
        dev.logger.info('Linda is leaving')
        return dev.exit()

    connect()
    connected = True
    for i in range(200):
        if i % 20 == 1:
            if connected:
                task = random.choice([get_int, get_float])
                if task == leave:
                    connected = False
            else:
                task = connect
                connected = True
            task()
            time.sleep(random.random())
        dev.loop()
    dev.logger.info('Linda program is ending')
    dev.exit()
