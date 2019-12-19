#!/home/kyle/anaconda3/bin/python
import names
from device import Device

import time
import tkinter as tk

if __name__ == "__main__":
    dev = Device(names.JOE, FLOAT=3.1415, INT=4)
    dev.start()

    gui_running = True

    top = tk.Tk()
    def on_closing():
        global gui_running
        gui_running = False

    top.protocol("WM_DELETE_WINDOW", on_closing)
    top.geometry("200x100")  
    float_lbl = tk.Label(top, text="FLOAT:")
    float_lbl.grid(row=0,column=0)
    float_val = tk.Label(top, text=str(dev.params['FLOAT']))
    float_val.grid(row=0,column=1)
    int_lbl = tk.Label(top, text="INT:")
    int_lbl.grid(row=1,column=0)
    int_val = tk.Label(top, text=str(dev.params['INT']))
    int_val.grid(row=1,column=1)

    while gui_running:
        if dev.loop() < 0:
            break
        float_val['text'] = str(dev.params['FLOAT'])
        int_val['text'] = str(dev.params['INT'])
        top.update_idletasks()
        top.update()
        # time.sleep(0.2)
    top.destroy()
    dev.exit()

