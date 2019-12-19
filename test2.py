

"""
Mail Broker
"""

from __future__ import print_function

import multiprocessing

import zmq

import names
import time
import random
import pickle
import uuid

NBR_DEVS = len(names.namelist)

class Device:
    def __init__(self, name, **kwargs):
        self.name = name
        self.params = kwargs
    
    def start(self, **kwargs):
        self.ctx = zmq.Context.instance()

        self.mailbox = self.ctx.socket(zmq.DEALER)
        self.mailbox.identity = '{}'.format(self.name).encode('utf-8')
        try:
            self.mailbox.connect(names.BROKER_IN)
        except zmq.ZMQBaseError as err:
            print('[{}] failed to connect to broker. Error {}'.format(self.name, err))

        self.poller = zmq.Poller()
        self.poller.register(self.mailbox, zmq.POLLIN)
        self.broker_alive = False

        self.cmd_queue = []
        self.sending = False
        self.sent_time = 0

        self.loop()

    def exit(self):
        if self.broker_alive:
            self.cmd_queue.clear()
            self.sending = False
            self.mailbox.send_multipart([b"BYE"])
        self.mailbox.close()
        self.ctx.term()
    
    def loop(self):
        # If the broker is not alive, try to connect to it
        if not self.broker_alive:
            self.mailbox.send_multipart([b"HELLO"])

            sockets = dict(self.poller.poll(1000))
            if self.mailbox in sockets:
                cmd = self.mailbox.recv_multipart()
                print('[{}] from broker: {}'.format(self.name, cmd))
                if cmd[0] == b"WELCOME":
                    self.broker_alive = True
                else:
                    print('[{}] Broker did not welcome me ({})'.format(self.name, cmd))
                    self.poller.unregister(self.mailbox)
                    return -1
        # If the broker is alive...
        if self.broker_alive:    
            sockets = dict(self.poller.poll(20))
            # Check mailbox only if we've sent a command
            if self.mailbox in sockets:
                msg = self.mailbox.recv_multipart()
                print('[{}] from broker: {}'.format(self.name, msg))
                if msg[0] == b"ACK":
                    if msg[1:] == self.cmd_queue[0]:
                        self.cmd_queue.pop(0)
                        self.sending = False
                    else:
                        print('[{}] received out of order message {}, discarding...'.format(self.name, msg))
                elif msg[0] == b"FAIL":
                    if msg[1:] == self.cmd_queue[0]:
                        self.cmd_queue.pop(0)
                        self.sending = False
                    else:
                        print('[{}] received out of order message {}, discarding...'.format(self.name, msg))
                elif msg[0] == b"GET":
                    prop = msg[1].decode('utf-8')
                    if prop in self.params:
                        reply = [b"RET", msg[1], pickle.dumps(self.params[prop]), msg[-1]]
                        self.mailbox.send_multipart(reply)
                        print('[{}] reply to broker with {}'.format(self.name, reply))
                elif msg[0] == b"RET":
                    identity = msg[1]
                    prop = msg[2].decode('utf-8')
                    value = pickle.loads(msg[3])
                    print('[{}] got {} = {} for device {}'.format(self.name, prop, value, identity))
                else:
                    print('[{}] did not understand message {}, discarding...'.format(self.name, msg))
            
            if self.sending:
                if time.time() - self.sent_time > 1:
                    print('[{}] Tried to send {}, but broker never ACK'.format(self.name, self.cmd_queue[0]))
                    self.broker_alive = False
                    self.reset_socket('mailbox', '', names.BROKER_IN)
                    self.cmd_queue.clear()
                    self.sending = False
            else:
                if len(self.cmd_queue) > 0:
                    print('[{}] Sending {} to broker'.format(self.name, self.cmd_queue[0]))
                    self.mailbox.send_multipart(self.cmd_queue[0])
                    self.sending = True
                    self.sent_time = time.time()
        
        return 0
    
    def send(self, msg):
        if self.broker_alive:
            self.cmd_queue.append(msg)
            print('[{}] Queue is {}'.format(self.name, self.cmd_queue))
    
    def reset_socket(self, sock, sockname, endpoint):
        self.__getattribute__(sock).setsockopt(zmq.LINGER, 0)
        self.__getattribute__(sock).close()
        try:
            self.poller.unregister(self.__getattribute__(sock))
        except KeyError:
            pass
        self.__setattr__(sock, self.ctx.socket(zmq.REQ))
        if len(sockname) > 0:
            sockname = '-' + sockname
        self.__getattribute__(sock).identity = '{}{}'.format(self.name, sockname).encode('utf-8')
        self.__getattribute__(sock).connect(endpoint)
        self.poller.register(self.__getattribute__(sock))

def joe():
    dev = Device(names.JOE, FLOAT=3.1415, INT=4)
    dev.start()
    while True:
        if dev.loop() < 0:
            break
        time.sleep(0.2)
        # i = random.randint(1, 10)
        # #print('{}: rolled {}'.format(name, i))
        # if i == 1 and running:
        #     dev.exit()
        #     running = False
        # elif i == 10 and not running:
        #     dev.start()
        #     running = True
        # elif running:
        #     if dev.loop() < 0:
        #         break
        # time.sleep(0.2)
    dev.exit()

def linda():
    dev = Device(names.LINDA)
    dev.start()
    running = True
    while True:
        i = random.randint(1, 10)
        if i == 1 and running:
            print('Linda is leaving!')
            dev.exit()
            running = False
        if i in [2, 3, 4, 5] and not running:
            print('Linda is restarting')
            dev.start()
            running = True
        elif i == 2 or i == 3:
            print("Linda wants to GET JOE's INT")
            dev.send([b"JOE", b"GET", b"INT"])
        elif i == 4 or i == 5:
            print("Linda wants to GET JOE's FLOAT")
            dev.send([b"JOE", b"GET", b"FLOAT"])
        if running:
            if dev.loop() < 0:
                break
        time.sleep(0.5)
    dev.exit()

def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.identity = 'BROKER'.encode('utf-8')
    frontend.bind(names.BROKER_IN)

    linda_proc = multiprocessing.Process(target=linda)
    linda_proc.daemon = True
    linda_proc.start()
    joe_proc = multiprocessing.Process(target=joe)
    joe_proc.daemon = True
    joe_proc.start()

    # Initialize main loop state
    count = NBR_DEVS
    devs = set()
    poller = zmq.Poller()

    poller.register(frontend, zmq.POLLIN)

    mail_table = {}

    def print_connections():
        for x in mail_table:
            if time.time() - x[2] > 5:
                print('[BROKER] Message from {} to {} is getting old'.format(x[0], x[1]))
        print('[BROKER] connected devices: {}'.format(devs))

    tasks = [print_connections]
    times = [1]
    timers = times.copy()

    while True:
        start_time = time.time()

        sockets = dict(poller.poll(20))

        if frontend in sockets:
            # Messages from REQ sockets from clients
            msg = frontend.recv_multipart()
            print('[BROKER] received: {}'.format(msg))
            from_addr = msg[0]
            cmd = msg[1:]
            if cmd[0] == b"HELLO":
                if from_addr in devs:
                    print("[BROKER] {} tried to join, but it already joined".format(from_addr))
                    msg = [from_addr, b"REFUSED"]
                    frontend.send_multipart(msg)
                    print('[BROKER] sending {}'.format(msg))
                else:
                    devs.add(from_addr)
                    msg = [from_addr, b"WELCOME"]
                    frontend.send_multipart(msg)
                    print('[BROKER] sending {}'.format(msg))
            elif cmd[0] == b"BYE":
                try:
                    devs.remove(from_addr)
                    for x in mail_table:
                        if x[0] == from_addr:
                            del x
                except KeyError:
                    print('[BROKER] received BYE from {} but {} is not listed in devs'.format(from_addr, from_addr))
                    pass
            else:
                if cmd[-1] in mail_table:
                    # this should be a reply to a message
                    mail_to, check_from, _ = mail_table[cmd[-1]]
                    if check_from == from_addr:
                        if cmd[0] == b"RET":
                            # This is a reply to a get command
                            prop = cmd[1]
                            value = cmd[2]
                            msg = [mail_to, b"RET", from_addr, prop, value]
                        else:
                            print('[BROKER] Message {} not yet implemented'.format(cmd))
                            msg = [b""]
                        frontend.send_multipart(msg)
                        del mail_table[cmd[-1]]
                        print('[BROKER] sending {}'.format(msg))
                    else:
                        print('[BROKER] received message {} but it came from {} and should have come from {}'.format(cmd, from_addr, check_from))
                else:
                    # this should be a request to send a message
                    to_addr = cmd[0]
                    if to_addr in devs:
                        # Forward message to destination
                        msg_id = uuid.uuid4().bytes
                        msg = [to_addr] + cmd[1:] + [msg_id]
                        try:
                            frontend.send_multipart(msg)
                            mail_table[msg_id] = (from_addr, to_addr, time.time())
                            print(mail_table)
                            print('[BROKER] sending {}'.format(msg))
                            try:
                                # Acknowledge message success
                                msg = [from_addr, b"ACK"] + cmd
                                frontend.send_multipart(msg)
                                print('[BROKER] sending {}'.format(msg))
                            except zmq.ZMQBaseError as err:
                                print('[BROKER] failed to send {} with error: {}'.format(msg, err))
                        except zmq.ZMQBaseError as err:
                            print('[BROKER] failed to send {} with error: {}'.format(msg, err))
                    else:
                        msg = [from_addr, b"FAIL"] + cmd
                        frontend.send_multipart(msg)
                        print('[BROKER] sending {}'.format(msg))
        
        end_time = time.time()
        dt = end_time - start_time
        timers = [x - dt for x in timers]
        for i, x in enumerate(timers):
            if x < 0:
                tasks[i]()
                timers[i] = times[i]
    
    while True:
        pass

    # Clean up
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()
