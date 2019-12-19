#!/home/kyle/anaconda3/bin/python
import names
from device import Device

import random
import time
import tkinter as tk

if __name__ == "__main__":
    dev = Device(names.LINDA, INT=6)
    dev.start()

    gui_running = True

    top = tk.Tk()
    def on_closing():
        global gui_running
        gui_running = False
    
    def get_joe_int():
        dev.send([b"JOE", b'GET', b"INT"])
    
    def get_joe_float():
        dev.send([b'JOE', b'GET', b'FLOAT'])
    
    def set_joe_float():
        dev.send([b'JOE', b'SET', b'FLOAT', pickle.dumps()])

    top.protocol("WM_DELETE_WINDOW", on_closing)
    top.geometry("200x100")  
    int_lbl = tk.Label(top, text="INT:")
    int_lbl.grid(row=1,column=0)
    int_val = tk.Label(top, text=str(dev.params['INT']))
    int_val.grid(row=1,column=1)
    button_joe_int = tk.Button(top, text="Get Joe INT", command=get_joe_int)
    button_joe_float = tk.Button(top, text="Get Joe FLOAT", command=get_joe_float)
    button_set_joe_float = tk.Button(top, text="Set Joe FLOAT", command=set_joe_float)

    while gui_running:
        if dev.loop() < 0:
            break
        int_val['text'] = str(dev.params['INT'])
        top.update_idletasks()
        top.update()
        # time.sleep(0.2)
    top.destroy()
    dev.exit()
