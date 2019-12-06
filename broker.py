#!/home/kyle/anaconda3/bin/python
import zmq
from MsgID import gen_id

import names
import time
import random
import logger
import errno

NBR_DEVS = names.NBR_DEVS

app_log = logger.make_logger('broker.log')

"""
mail_table is a dictionary
the key is a uuid generated by uuid.uuid4(), used to uniquely identify messages
the value for each key is a tuple (from_addr, to_addr, msg)

devs is a python set which contain bytes representation of names
"""

def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.identity = 'BROKER'.encode('utf-8')
    frontend.bind(names.BROKER_IN)

    # Initialize main loop state
    count = NBR_DEVS
    devs = set()
    poller = zmq.Poller()

    poller.register(frontend, zmq.POLLIN)

    mail_table = {}

    def print_connections():
        for id, (from_addr, to_addr, timestamp, msg) in mail_table.items():
            if time.time() - timestamp > 5:
                app_log.debug('Message from {} to {} is older than 5 seconds'.format(from_addr.decode('utf-8'), to_addr.decode('utf-8')))
        if len(devs) > 0:
            app_log.info('connected devices: {}'.format(devs))
        else:
            app_log.info('no connected devices.')

    def print_mail_table(mt):
        header_format = '{0:<34} : {1}\n'
        row_format = '0x{0} : {1}\n'
        mt_str = '\n'
        mt_str += header_format.format('Msg ID', '(From, To, Timestamp, Msg)')
        for id, msg in mt.items():
            mt_str += row_format.format(id.hex(), msg)
        return mt_str

    tasks = [print_connections]
    times = [1]
    timers = times.copy()

    while True:
        start_time = time.time()

        sockets = dict(poller.poll(20))

        if frontend in sockets:
            # msg will be [socket identity, b'', b'HI' or b'BYE' or msg_id]
            msg = frontend.recv_multipart()
            app_log.info('received: {}'.format(msg))
            from_addr = msg[0]
            cmd = msg[2]
            """
            Handle messages from workers to broker
            """
            if cmd == b"HI":
                if from_addr in devs:
                    app_log.warning("{} tried to join, but it already joined".format(from_addr))
                    msg = [from_addr, b"", b"ERR", b"Device already connected"]
                else:
                    devs.add(from_addr)
                    msg = [from_addr, b"", b"OK"]
                frontend.send_multipart(msg)
                app_log.debug('sending {}'.format(msg))
            elif cmd == b"BYE":
                try:
                    devs.remove(from_addr)
                    for x in mail_table:
                        if x[0] == from_addr:
                            del x
                except KeyError:
                    app_log.warning('received BYE from {} but {} is not listed in devs'.format(from_addr, from_addr))
            else:
                """
                Message should begin with msg_id, possibly dest, then (GET, SET, RET, MET), then extra info
                out_msg is the message sent to to_addr
                reply is the message sent to from_addr
                """
                msg_id = msg[2]
                msg = msg[3:]  # strip everything up to and including msg_id
                out_msg = None  # goes to the to_addr, which is extracted from message or mail_table
                reply = None  # goes to the from_addr
                if msg_id not in mail_table:  # this could be a new request
                    to_addr = msg[0]
                    cmd = msg[1]
                    msg = msg[1:]  # strip the to_addr
                    if to_addr in devs:
                        if cmd == b'GET':
                            out_msg = [to_addr, b'', msg_id] + msg
                            reply = [from_addr, b'', msg_id, b'ACK']
                            mail_table[msg_id] = (from_addr, to_addr, time.time(), msg)
                            app_log.debug('Processed GET')
                        elif cmd == b'SET':
                            out_msg = [to_addr, b'', msg_id] + msg
                            reply = [from_addr, b'', msg_id, b'ACK']
                            mail_table[msg_id] = (from_addr, to_addr, time.time(), msg)
                            app_log.debug('Processed SET')
                        else:
                            reply = [from_addr, b'', msg_id, b'ERR', b'Command not understood']
                            app_log.warning('command {} not yet supported'.format(cmd))
                    else:
                        reply = [from_addr, b'', msg_id, b'ERR', b'Device not connected']
                        app_log.debug('requested device {} does not exist'.format(to_addr))
                        app_log.debug(print_mail_table(mail_table))
                else:  # this could be a reply to a request
                    to_addr = mail_table[msg_id][0]  # lookup message requestor
                    cmd = msg[0]
                    if to_addr in devs:
                        if from_addr != mail_table[msg_id][1]:
                            app_log.critical('{} sent a message ID that does not agree with mail table.'.format(from_addr))
                            app_log.critical(msg)
                            app_log.critical(print_mail_table(mail_table))
                        elif cmd == b'RET':
                            out_msg = [to_addr, b'', msg_id] + msg
                            app_log.debug('Processed RET')
                        elif cmd == b'MET':
                            out_msg = [to_addr, b'', msg_id] + msg
                            app_log.debug('Processed MET')
                        elif cmd == b'ERR':
                            out_msg = [to_addr, b'', msg_id] + msg
                        else:
                            out_msg = [to_addr, b'', msg_id, b'ERR', b'Device replied poorly']
                            app_log.warning('{} sent unrecognized response: {}'.format(from_addr, msg))
                    else:
                        app_log.warning('original requestor {} no longer connected'.format(to_addr))
                if out_msg is not None:
                    try:
                        frontend.send_multipart(out_msg)
                        app_log.debug('sending {}'.format(out_msg))
                    except zmq.ZMQBaseError as err:
                        app_log.debug('failed to send {} with error: {}'.format(msg, err))
                if reply is not None:
                    try:
                        frontend.send_multipart(reply)
                        app_log.debug('sending {}'.format(reply))
                    except zmq.ZMQBaseError as err:
                        app_log.debug('failed to send {} with error: {}'.format(msg, err))

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

