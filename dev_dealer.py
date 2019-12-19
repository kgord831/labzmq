import zmq
import logger
import os
import time

RT = {
    'SAM': 'tcp://127.0.0.1:5560',
    'ERIC': 'tcp://127.0.0.1:5561',
    'FRED': 'tcp://127.0.0.1:5562'
}

ID_LEN = 2

class Message():
    def __init__(self, msg, msg_id=None, timeout=1, sent=False, sent_time=0):
        self.msg = msg
        if msg_id is None:
            self.msg_id = self.counter
        else:
            self.msg_id = msg_id
        self.timeout = timeout
        self.sent = sent
        self.sent_time = sent_time
    def __repr__(self):
        s = "Message(msg={},id={},timeout={},sent={},sent_time={})".format(self.msg, self.msg_id, self.timeout, self.sent, self.sent_time)
        return s

class Device():
    def __init__(self, name):
        if name not in RT.keys():
            raise KeyError
        self.name = name
        self.ctx = zmq.Context.instance()
        self.make_rep_socket()  # make inbox
        self.make_req_socket()  # make outbox
        self.poller = zmq.Poller()
        self.reqs = {}
        self._counter = 0

    def make_req_socket(self):
        self.outbox = self.ctx.socket(zmq.DEALER)
        self.outbox.setsockopt(zmq.LINGER, 0)

    def make_rep_socket(self):
        self.inbox = self.ctx.socket(zmq.REP)
        self.inbox.setsockopt(zmq.LINGER, 0)

    @property
    def counter(self):
        self._counter += 1
        return self._counter.to_bytes(ID_LEN, 'big')

    def connect(self):
        try:
            self.inbox.bind('{}'.format(RT[self.name]))
        except zmq.ZMQBaseError as err:
            raise err
        for dev_name, addr in RT.items():
            if dev_name != self.name:
                try:
                    self.outbox.connect(addr)
                except zmq.ZMQBaseError as err:
                    raise err
        self.poller.register(self.inbox, zmq.POLLIN)
        self.poller.register(self.outbox, zmq.POLLIN)
        print('device connected')

    def disconnect(self):
        try:
            self.poller.unregister(self.inbox)
        except KeyError:
            pass
        try:
            self.poller.unregister(self.outbox)
        except KeyError:
            pass
        self.inbox.close()
        self.outbox.close()
        self.inbox = make_req_socket(self.ctx)
        self.outbox = make_rep_socket(self.ctx)
        print('device disconnected')

    def reset(self):
        self.disconnect()
        self.connect()


    def send(self, msg, dest, timeout=1):
        msg_id = self.counter
        if isinstance(msg, list):
            dat = [b"", dest.encode('utf-8'), msg_id] + msg
        elif isinstance(msg, str):
            dat = [b"", dest.encode('utf-8'), msg_id, msg.encode('utf-8')]
        else:
            dat = [b"", dest.encode('utf-8'), msg_id, msg]
        send_msg = Message(dat, msg_id, timeout)
        print("Sending: {}".format(send_msg))
        self.reqs[msg_id] = send_msg

    def handle_reply(self, msg):
        msg_id = msg[0]
        cmd = msg[1].decode('utf-8')
        reply = [msg_id]
        if cmd == 'Hi':
            reply += [b'Hello']
        else:
            reply += [b'Error', b'Not understood']
        self.inbox.send_multipart(reply)
        print('Replied with {}'.format(reply))

    def loop(self):
        sockets = dict(self.poller.poll(20))
        if self.inbox in sockets:
            msg = self.inbox.recv_multipart()
            if msg[0].decode('utf-8') == self.name:
                print('Received message: {}'.format(msg[1:]))
                self.handle_reply(msg[1:])
            else:
                print('Received message not addressed to me: {}'.format(msg[1:]))
        if self.outbox in sockets:
            msg = self.outbox.recv_multipart()
            print('Received a reply: {}'.format(msg))
            msg_id = msg[1]
            msg = msg[2:]
            if msg_id in self.reqs:
                print('Received a reply from a known request')
                del self.reqs[msg_id]
            else:
                print('Received a message I do not remember sending.')
        for msg_id, msg in self.reqs.items():
            if not msg.sent:
                self.outbox.send_multipart(msg.msg)
                msg.sent = True
                msg.sent_time = time.time()
            elif time.time() - msg.sent_time > msg.timeout:
                print('{} is old'.format(msg))
        self.reqs = dict((msg_id, msg) for msg_id, msg in self.reqs.items() if not msg.sent or time.time() - msg.sent_time < msg.timeout)

if __name__ == '__main__':
    sam = Device('SAM')
    sam.connect()
    eric = Device('ERIC')
    eric.connect()
    fred = Device('FRED')
    fred.connect()
    sam.send("Hi", "ERIC", 0.3)
    fred.send("Hi", "ERIC", 0.3)
    eric.send("Hi", "FRED", 0.3)
    for i in range(10):
        print('--------SAM---------')
        sam.loop()
        print('-------ERIC---------')
        eric.loop()
        print('-------FRED---------')
        fred.loop()
