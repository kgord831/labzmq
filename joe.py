#!/home/kyle/anaconda3/bin/python
import names
from device import Device

import time

if __name__ == "__main__":
    dev = Device(names.JOE, FLOAT=3.1415, INT=4)
    dev.start()
    while True:
        if dev.loop() < 0:
            break
        # time.sleep(0.2)
    dev.exit()

