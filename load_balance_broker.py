import threading
import zmq
import time

NBR_CLIENTS = 10
NBR_WORKERS = 3

def client_task(ident, context=None):
    # Prepare shared context
    context = context or zmq.Context.instance()
    # Prepare socket for req-rep
    socket = context.socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode('ascii')
    socket.connect("inproc://frontend")
    # Prepare socket for direct control
    controller = context.socket(zmq.SUB)
    controller.connect("tcp://localhost:5555")
    controller.setsockopt(zmq.SUBSCRIBE, b"")
    # Prepare poller
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(controller, zmq.POLLIN)
    # Send request, get reply
    socket.send(b"HELLO")

    while True:
        sockets = dict(poller.poll())
        if socket in sockets:
            reply = socket.recv()
            print("%s : %s" % (socket.identity, reply))
        if controller in sockets:
            break

    socket.close()
    controller.close()

def worker_task(ident, context=None):
    # Prepare context
    context = context or zmq.Context.instance()
    # Prepare socket for connections
    socket = context.socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode('ascii')
    socket.connect("inproc://backend")
    # Prepare socket for direct control
    controller = context.socket(zmq.SUB)
    controller.connect("tcp://localhost:5555")
    controller.setsockopt(zmq.SUBSCRIBE, b"")
    # Prepare poller
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(controller, zmq.POLLIN)

    # Tell broker we're ready for work
    socket.send(b"READY")

    while True:
        socks = dict(poller.poll())
        if socket in socks:
            address, empty, request = socket.recv_multipart()
            print("%s : %s" % (socket.identity, request))
            socket.send_multipart([address, b"", b"OK"])
        if controller in socks:
            break
    socket.close()
    controller.close()

def main():
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("inproc://frontend")
    backend = context.socket(zmq.ROUTER)
    backend.bind("inproc://backend")
    controller = context.socket(zmq.PUB)
    controller.bind("tcp://*:5555")
    time.sleep(0.5)

    # Start background tasks
    def start(task, *args):
        thread = threading.Thread(target=task, args=args)
        thread.daemon = True
        thread.start()
    for i in range(NBR_CLIENTS):
        start(client_task, i, context)
    for i in range(NBR_WORKERS):
        start(worker_task, i, context)

    # Initialize main loop
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
                emtpy, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
                count -= 1
                if not count:
                    break
        if frontend in sockets:
            # Get next client request, route to last-used worker
            client, empty, request = frontend.recv_multipart()
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)
    # We are done. Signal all processes to terminate
    for i in range(NBR_CLIENTS):
        controller.send(b"")
    # Wait for processes to close connections and die
    time.sleep(0.5)
    # Clean up
    backend.close()
    frontend.close()
    controller.close()
    context.term()

if __name__ == "__main__":
    main()
