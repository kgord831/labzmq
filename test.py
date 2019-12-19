

"""
Mail Broker
"""

from __future__ import print_function

import multiprocessing

import zmq

import names
import time
import random

NBR_DEVS = names.NBR_DEVS

class Device:
    def __init__(self, name, **kwargs):
        self.name = name
        self.params = kwargs
    
    def start(self, **kwargs):
        print('{} is starting'.format(self.name))
        self.ctx = zmq.Context.instance()

        self.inbox = self.ctx.socket(zmq.REQ)
        self.inbox.identity = '{}-INBOX'.format(self.name).encode('utf-8')
        self.inbox.connect(names.BROKER_OUT)

        self.outbox = self.ctx.socket(zmq.REQ)
        self.outbox.identity = '{}-OUTBOX'.format(self.name).encode('utf-8')
        self.outbox.connect(names.BROKER_IN)

        self.poller = zmq.Poller()
        self.poller.register(self.inbox)
        self.poller.register(self.outbox)
        self.broker_alive = False

        self.cmd_queue = []
        self.sending = False
        self.sent_time = 0

        self.loop()

    def exit(self):
        if self.broker_alive:
            if self.sending:
                sockets = dict(self.poller.poll(1000))
                if self.outbox in sockets:
                    self.outbox.recv_multipart()
            self.cmd_queue.clear()
            self.sending = False
            self.outbox.send_multipart([b"BYE"])
        self.outbox.close()
        self.inbox.close()
        self.ctx.term()
    
    def loop(self):
        # If the broker is not alive, try to connect to it
        if not self.broker_alive:
            self.inbox.send_multipart([b"HELLO"])
            self.outbox.send_multipart([b"HELLO"])

            sockets = dict(self.poller.poll(1000))
            if self.outbox in sockets:
                cmd = self.outbox.recv_multipart()
                print('[{}-OUTBOX] from broker: {}'.format(self.name, cmd))
                if cmd[0] == b"WELCOME":
                    self.broker_alive = True
                else:
                    print('[{}] Broker did not welcome me ({})'.format(self.name, cmd))
                    self.poller.unregister(self.outbox)
                    return -1
            elif self.inbox in sockets:
                cmd = self.inbox.recv_multipart()
                print('[{}-INBOX] from broker: {}'.format(self.name, cmd))
                if cmd[0] == b"WELCOME":
                    self.broker_alive = True
                else:
                    print('[{}] Broker did not welcome me ({})'.format(self.name, cmd))
                    self.poller.unregister(self.outbox)
                    return -1
        # If the broker is alive...
        if self.broker_alive:    
            sockets = dict(self.poller.poll(20))
            # Check outbox only if we've sent a command
            if self.outbox in sockets and self.sending:
                msg = self.outbox.recv_multipart()
                print('[{}-OUTBOX] from broker: {}'.format(self.name, msg))
                if msg[0] == b"ACK":
                    if msg[1:] == self.cmd_queue[0]:
                        self.cmd_queue.pop(0)
                        self.sending = False
                    else:
                        print('[{}-OUTBOX] received out of order message {}, discarding...'.format(self.name, msg))
                elif msg[0] == b"FAIL":
                    if msg[1:] == self.cmd_queue[0]:
                        self.cmd_queue.pop(0)
                        self.sending = False
                    else:
                        print('[{}-OUTBOX] received out of order message {}, discarding...'.format(self.name, msg))
                else:
                    print('[{}-OUTBOX] did not understand message {}, discarding...'.format(self.name, msg))
            # Check inbox for requests from broker
            if self.inbox in sockets:
                cmd = self.inbox.recv_multipart()
                print('[{}-INBOX] from broker: {}'.format(self.name, cmd))
                self.inbox.send_multipart([b"ACK"])
            
            if self.sending:
                if time.time() - self.sent_time > 1:
                    print('[{}] Tried to send {}, but broker never ACK'.format(self.name, self.cmd_queue[0]))
                    self.broker_alive = False
                    self.reset_socket('outbox', 'OUTBOX', names.BROKER_IN)
                    self.cmd_queue.clear()
                    self.sending = False
            else:
                if len(self.cmd_queue) > 0:
                    print('[{}] Sending {} to broker'.format(self.name, self.cmd_queue[0]))
                    self.outbox.send_multipart(self.cmd_queue[0])
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
        self.__getattribute__(sock).identity = '{}-{}'.format(self.name, sockname).encode('utf-8')
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
    print('Linda Started')
    running = True
    while True:
        i = random.randint(1, 10)
        print('Linda rolled {}'.format(i))
        if i == 1 and running:
            dev.exit()
            running = False
        if i in [2, 3, 4, 5] and not running:
            dev.start()
            running = True
        elif i == 2 or i == 3:
            dev.send([b"JOE", b"GET", b"INT"])
        elif i == 4 or i == 5:
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
    frontend.identity = 'BROKER-IN'.encode('utf-8')
    frontend.bind(names.BROKER_IN)
    backend = context.socket(zmq.ROUTER)
    backend.identity = 'BROKER-OUT'.encode('utf-8')
    backend.bind(names.BROKER_OUT)

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

    def print_connections():
        # msg = [b"JOE-INBOX", b"", b"HELLO"]
        # backend.send_multipart(msg)
        # print('[BROKER-OUT] sent {}'.format(msg))
        print('[BROKER] connected devices: {}'.format(devs))

    tasks = [print_connections]
    times = [1]
    timers = times.copy()

    def get_dev_name(outbox_name):
        return (outbox_name.decode('utf-8').split('-')[0]).encode('utf-8')
    
    def make_inbox(dev_name):
        return (dev_name.decode('utf-8') + '-INBOX').encode('utf-8')
    
    def make_outbox(dev_name):
        return (dev_name.decode('utf-8') + '-OUTBOX').encode('utf-8')

    while True:
        start_time = time.time()

        sockets = dict(poller.poll(20))

        if backend in sockets:
            # Replies from REP sockets from clients
            msg = backend.recv_multipart()
            print('[BROKER-OUT] recevied: {}'.format(msg))
            # worker, _, client = request[:3]
            # if not workers:
            #     # Poll for clients now that a worker is available
            #     poller.register(frontend, zmq.POLLIN)
            # workers.append(worker)
            # if client != b"READY" and len(request) > 3:
            #     # If client reply, send rest back to frontend
            #     _, reply = request[3:]
            #     frontend.send_multipart([client, b"", reply])
            #     count -= 1
            #     if not count:
            #         break

        if frontend in sockets:
            # Messages from REQ sockets from clients
            msg = frontend.recv_multipart()
            print('[BROKER-IN] recevied: {}'.format(msg))
            from_addr = msg[0]
            cmd = msg[2:]
            if cmd[0] == b"HELLO":
                if get_dev_name(from_addr) in devs:
                    print("[BROKER] {} tried to join, but it already joined".format(get_dev_name(from_addr)))
                    msg = [from_addr, b"", b"REFUSED"]
                    frontend.send_multipart(msg)
                    print('[BROKER-IN] sending {}'.format(msg))
                else:
                    devs.add(get_dev_name(from_addr))
                    msg = [from_addr, b"", b"WELCOME"]
                    frontend.send_multipart(msg)
                    print('[BROKER-IN] sending {}'.format(msg))
            elif cmd[0] == b"BYE":
                try:
                    devs.remove(get_dev_name(from_addr))
                except KeyError:
                    print('[BROKER] received BYE from {} but {} is not listed in devs'.format(from_addr, get_dev_name(from_addr)))
                    pass
            else:
                to_addr = cmd[0]
                if to_addr in devs:
                    # Forward message to destination
                    msg = [make_inbox(to_addr), b""] + cmd[1:]
                    try:
                        backend.send_multipart(msg)
                        print('[BROKER-OUT] sending {}'.format(msg))
                        try:
                            # Acknowledge message success
                            msg = [from_addr, b"", b"ACK"] + cmd
                            frontend.send_multipart(msg)
                            print('[BROKER-IN] sending {}'.format(msg))
                        except zmq.ZMQBaseError as err:
                            print('[BROKER-IN] failed to send {} with error: {}'.format(msg, err))
                    except zmq.ZMQBaseError as err:
                        print('[BROKER-OUT] failed to send {} with error: {}'.format(msg, err))
                else:
                    msg = [from_addr, b"", b"FAIL"] + cmd
                    frontend.send_multipart(msg)
                    print('[BROKER-IN] sending {}'.format(msg))
        
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
    backend.close()
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()
