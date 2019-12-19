

"""
Load-balancing broker

Clients and workers are shown here in-process.

Author: Brandon Carpenter (hashstat) <brandon(dot)carpenter(at)pnnl(dot)gov>
"""

from __future__ import print_function

import threading

import zmq

NBR_CLIENTS = 10
NBR_WORKERS = 3

def client_task(ident, context):
    """Basic request-reply client using REQ socket."""
    socket = context.socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("inproc://a")

    # Send request, get reply
    socket.send_multipart([b"HELLO"])
    reply = socket.recv()
    print("{}: {}".format(socket.identity.decode("ascii"),
                          reply.decode("ascii")))
    while True:
        pass

def worker_task(ident, context):
    """Worker task, using a REQ socket to do load-balancing."""
    socket = context.socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("inproc://b")

    # Tell broker we're ready for work
    socket.send(b"READY")

    while True:
        address, empty, request = socket.recv_multipart()
        print("{}: {}".format(socket.identity.decode("ascii"),
                              request.decode("ascii")))
        socket.send_multipart([address, b"", b"OK"])

def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("inproc://a")
    backend = context.socket(zmq.ROUTER)
    backend.bind("inproc://b")

    # Start background tasks
    def start(task, *args):
        process = threading.Thread(target=task, args=args)
        process.daemon = True
        process.start()
    for i in range(NBR_CLIENTS):
        start(client_task, i, context)
    for i in range(NBR_WORKERS):
        start(worker_task, i, context)

    # Initialize main loop state
    count = NBR_CLIENTS
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())

        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            worker, empty, client = request[:3]
            if not workers:
                # Poll for clients now that a worker is available
                poller.register(frontend, zmq.POLLIN)
            workers.append(worker)
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
                count -= 1
                if not count:
                    break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            #client, empty, request = frontend.recv_multipart()
            client, empty, request = frontend.recv_multipart()
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)
    
    while True:
        pass

    # Clean up
    backend.close()
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()
