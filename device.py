import names
from MsgID import gen_id

import zmq
import random
import pickle
import time
import logger
import os  # urandom function
from collections import OrderedDict

states = ['closed', 'nobroker', 'joining', 'rejected', 'idle', 'leaving']

ID_LEN = 4

def make_socket(ctx, name):
    """A utility function that constructs the Dealer socket used by the device"""
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.identity = str(name).encode('utf-8')
    return sock


class DeviceNotConnected(Exception):
    def __init__(self):
        Exception.__init__(self, "Target device is not connected to network.")


class Command(object):
    """A command for a device contains a zmq msg, a timeout in seconds, a boolean state indicating if msg is sent, and a POSIX time when the message is sent"""
    def __init__(self, msg_id=None, msg=b"", timeout=1):
        if msg_id is None:
            self.msg_id = gen_id()
        self.msg = msg
        self.timeout = timeout
        self.sent = False
        self.sent_time = -1

    def __repr__(self):
        s = "msg={},timeout={},sent={},sent_time={}".format(self.msg, self.timeout, self.sent, self.sent_time)
        return s
    
    def get_dest(self):
        return self.msg[0].decode('utf-8')


class CommandQueue(object):
    """A simple object wrapping a queue dictionary with a custom filter function"""
    def __init__(self):
        self.queue = {}

    def __repr__(self):
        return self.queue.__repr__()

    def __str__(self):
        s = "\n"
        row_fmt = "{0:<32} {1:<5} {2:<20}\n"
        s += row_fmt.format('ID', 'Sent', 'Sent Time')
        for msg_id, msg in self.queue.items():
            s += row_fmt.format(msg_id.hex(), msg.sent, msg.sent_time)
        return s[:-1]

    def __getitem__(self, key):
        return self.queue[key]

    def __setitem__(self, key, value):
        self.queue[key] = value

    def __contains__(self, key):
        return self.queue.__contains__(key)

    def pop(self, key):
        return self.queue.pop(key)

    def clear(self):
        self.queue.clear()

    def items(self):
        return self.queue.items()

    def filter_expired(self):
        """Filter the queue by removing expired commands, log the expired entries"""
        self.queue = dict((msg_id, cmd) for msg_id, cmd in self.queue.items() if time.time() - cmd.sent_time < cmd.timeout)


class Device():
    def __init__(self, name, **kwargs):
        self.name = name
        self.params = kwargs
        self.running = False
        self.logger = logger.make_logger(name + '.log')
        self.ctx = zmq.Context.instance()
        self.mailbox = make_socket(self.ctx, name)
        self.poller = zmq.Poller()
        self.cmd_queue = CommandQueue()
        self.state = 'closed'

    def connect(self):
        try:
            self.mailbox.connect(names.BROKER_IN)
        except zmq.ZMQBaseError as err:
            raise err
        self.poller.register(self.mailbox, zmq.POLLIN)
        self.logger.debug('device connected')
        self.logger.debug(self.cmd_queue)

    def disconnect(self):
        try:
            self.poller.unregister(self.mailbox)
        except KeyError:
            pass
        self.mailbox.close()
        self.mailbox = make_socket(self.ctx, self.name)  # pre-emptive in case user wants to connect again
        self.logger.debug('device disconnected')

    def reset_connection(self):
        self.disconnect()
        self.connect()

    def start(self, **kwargs):
        """
        Execute this to go from closed to nobroker state
        If the device is not in the closed state, this does nothing
        """
        if self.state == 'closed':
            try:
                self.connect()
                self.state = 'nobroker'
                self.cmd_queue.clear()
            except zmq.ZMQBaseError as err:
                self.logger.critical('Failed to connect to socket. Error {}'.format(err))
                raise err

        return 0

    def exit(self):
        """Execute this to disconnect from broker, blocks until device is in closed state"""
        if self.state == 'idle' or self.state == 'joining':
            self.state = 'leaving'
        elif self.state != 'closed':
            self.state = 'closing'
        while True:
            self.loop()
            if self.state == 'closed':
                break
        return 0


    def broker_alive(self):
        """Convenience function for checking if broker is alive or not"""
        return self.state == 'idle' or self.state == 'rejected' or self.state == 'leaving'

    def check_inbox(self):
        """Poll for messages, parse incoming messages from broker"""
        sockets = dict(self.poller.poll(20))
        if self.mailbox in sockets:
            msg = self.mailbox.recv_multipart()
            self.logger.debug('recv from broker: {}'.format(msg))
            msg_id = msg[1]
            cmd = msg[2]
            msg = msg[2:]  # msg now starts with CMD
            if cmd == b'ERR':
                error_msg = msg[1].decode('utf-8')
                if error_msg == "Device not connected":
                    if msg_id in self.cmd_queue:
                        self.logger.warning('{} not connected'.format(self.cmd_queue[msg_id]))
                    else:
                        self.logger.debug('Broker said a device was not connected, but no msg_id in queue')
                else:
                    self.logger.warning('Error: {}'.format(error_msg))
                self.cmd_queue.pop(msg_id)
                return
            if msg_id in self.cmd_queue:
                if cmd == b"ACK":
                    self.logger.info('broker acknowledged receipt of message')
                elif cmd == b"RET":
                    param = msg[1].decode('utf-8')
                    value = pickle.loads(msg[2])
                    self.logger.info('got {} = {} from {}'.format(param, value, self.cmd_queue[msg_id].get_dest()))
                    self.cmd_queue.pop(msg_id)
                elif cmd == b'MET':
                    self.logger.info('{} successfully set {}'.format(self.cmd_queue[msg_id].get_dest(), self.cmd_queue[msg_id].msg[1].decode('utf-8')))
                else:
                    self.logger.warning('did not understand message {}, discarding...'.format(msg))
            else:
                if cmd == b"GET":
                    param = msg[1].decode('utf-8')
                    if param in self.params:
                        reply = [b"", msg_id, b'RET', msg[1], pickle.dumps(self.params[param])]
                    else:
                        reply = [b'', msg_id, b'ERR', '{} is not param'.format(param).encode('utf-8')]
                    self.logger.info('reply to broker with {}'.format(reply))
                    self.mailbox.send_multipart(reply)
                elif cmd == b'SET':
                    param = msg[1].decode('utf-8')
                    if param in self.params:
                        self.params[param] = pickle.loads(msg[2])
                        reply = [b'', msg_id, b'MET', msg[1], msg[2]]
                    else:
                        reply = [b'', msg_id, b'ERR', '{} is not param'.format(param).encode('utf-8')]
                    self.logger.info('reply to broker with {}'.format(reply))
                    self.mailbox.send_multipart(reply)
                else:
                    self.logger.warning('did not understand: {}. Discarding...'.format(msg))

    def loop(self):
        """
        Run the code for a given state
        """
        if self.state == 'closed':
            return 1
        elif self.state == 'nobroker':
            msg = [b"", b'HI']
            self.logger.debug('sending: {}'.format(msg))
            self.mailbox.send_multipart(msg)
            self.state = 'joining'
        elif self.state == 'joining':
            sockets = dict(self.poller.poll(1000))
            if self.mailbox in sockets:
                cmd = self.mailbox.recv_multipart()
                self.logger.debug('received from broker: {}'.format(cmd))
                cmd = cmd[1:]  # strip b'' delimiter frame
                if cmd[0] == b'OK':
                    self.state = 'idle'
                elif cmd[0] == b'ERR' and cmd[1] == b"Device already connected":
                    self.logger.warning('Broker says I am already connected ({})'.format(cmd))
                    self.state = 'rejected'
                else:
                    self.logger.warning('Did not understand reply from broker: {}'.format(cmd))
            else:
                self.logger.warning('timed out trying to connect to broker')
                self.reset_connection()
                self.state = 'nobroker'
        elif self.state == 'rejected':
            return -1
        elif self.state == 'idle':
            # Run functions to update parameters
            # Check the inbox
            self.check_inbox()
            # Send messages
            for msg_id, cmd in self.cmd_queue.items():
                if cmd.sent:
                    if time.time() - cmd.sent_time >= cmd.timeout:
                        self.logger.warning('Message {} was sent [{}], but has timed out.'.format(cmd.msg, time.asctime(time.gmtime(cmd.sent_time))))
                else:
                    msg = [b'', cmd.msg_id] + cmd.msg
                    self.logger.debug('sending {}'.format(msg))
                    self.mailbox.send_multipart(msg)
                    cmd.sent = True
                    cmd.sent_time = time.time()
            self.cmd_queue.filter_expired()
        elif self.state == 'leaving':
            self.mailbox.send_multipart([b'', b'BYE'])
            self.state = 'closing'
        elif self.state == 'closing':
            self.cmd_queue.clear()
            self.disconnect()
            self.state = 'closed'
        else:
            self.logger.critical('The device is an unknown state: {}. This might be a typo in code.'.format(self.state)) 
            self.state = 'idle'           
        return 0
    
    def send(self, msg, timeout=1):
        """Put a message on the command queue with timeout in seconds"""
        cmd = Command(None, msg, timeout)
        self.cmd_queue[cmd.msg_id] = cmd
        self.logger.debug('Added msg to queue. Queue is {}'.format(self.cmd_queue))

    def reset_socket(self, sock, sockname, endpoint):
        """A generic reset_socket fcn taken from zmq guide"""
        self.__getattribute__(sock).setsockopt(zmq.LINGER, 0)
        self.__getattribute__(sock).close()
        try:
            self.poller.unregister(self.__getattribute__(sock))
        except KeyError:
            pass
        self.__setattr__(sock, self.ctx.socket(zmq.DEALER))
        if len(sockname) > 0:
            sockname = '-' + sockname
        self.__getattribute__(sock).identity = '{}{}'.format(self.name, sockname).encode('utf-8')
        self.__getattribute__(sock).setsockopt(zmq.LINGER, 100)
        self.__getattribute__(sock).connect(endpoint)
        self.poller.register(self.__getattribute__(sock))
        time.sleep(1)
