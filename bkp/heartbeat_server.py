from collections import OrderedDict
import time

import zmq

HEARTBEAT_LIVENESS = 4
HEARTBEAT_INTERVAL = 1   # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = b'\x01'      # Signals worker is ready
PPP_HEARTBEAT = b'\x02'  # Signals worker heartbeat


class Worker(object):
    def __init__(self, address):
        self.address   = address
        self.expiry    = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
        self.tasks     = list()
        self.max_tasks = 6

    def update_expiry(self):
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def send_heartbeat(self, backend):
        backend.send_string(self.address, flags=0 | zmq.SENDMORE)
        backend.send_string(PPP_HEARTBEAT)

    def update_tasks(self,task):
        # self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS
        self.tasks.append(task)
        # print(self.tasks, self)

    def remove_task(self):
        # print(self.tasks, self)
        self.tasks.pop(0)


class WorkerQueue(object):
    def __init__(self):
        # Queue is for all active free processes
        self.queue = OrderedDict()

        # Non_expired is for all alive processes
        self.non_expired = dict()

    def ready(self, address, add_queue = 0):

        # Check if the address was still in non expired queue
        if address in self.non_expired:
            self.non_expired[address].update_expiry()
            if add_queue == 1:
                self.queue[address] = self.non_expired[address]
        else:
            self.queue[address] = Worker(address)
            self.non_expired[address] = self.queue[address]
            self.queue[address].update_expiry()

    """Look for & kill expired workers."""
    def purge(self):
        t = time.time()
        expired = []

        # Get the list of expired worker's addresses in expired list
        n_exp = self.non_expired.items()
        for address, worker in n_exp:
            if t > worker.expiry:  # Worker expired
                expired.append(address)
            # print("{} Expiry time {} Worker {} expiry {}".format(expired, t, address, worker.expiry))


        temp = list()
        # Get the list of unfinished tasks on these expired workers for rerouting
        for address in expired:
            print("W: Idle worker expired: %s" % address)

            # Append tasks of expired workers to default queue
            temp.extend(self.non_expired[address].tasks)

            # first pop from the non_exp dict
            # work_obj = self.queue[address]
            self.non_expired.pop(address)

            # then pop from the queue
            self.queue.pop(address, None)

        # return the list of unfinished tasks
        return temp

    """  Get the next worker for assigning work"""
    def next(self):
        address, worker = self.queue.popitem(False)

        # Append to the end of idle worker queue if max_tasks not reached
        if len(worker.tasks) < worker.max_tasks:
            self.ready(address,1)
        return [address,worker]


    # address in string format
    def worker_at(self, address):
        if address in self.non_expired:
            return self.non_expired[address]
        else:
            return None

#
# context = zmq.Context(1)
#
# frontend = context.socket(zmq.ROUTER) # ROUTER
# backend = context.socket(zmq.ROUTER)  # ROUTER
# frontend.bind("tcp://0.0.0.0:5555") # For clients
# backend.bind("tcp://0.0.0.0:5556")  # For workers
#
# poll_workers = zmq.Poller()
# poll_workers.register(backend, zmq.POLLIN)
#
# poll_both = zmq.Poller()
# poll_both.register(frontend, zmq.POLLIN)
# poll_both.register(backend, zmq.POLLIN)
#
# workers = WorkerQueue()
#
# heartbeat_at = time.time() + HEARTBEAT_INTERVAL
#
# while True:
#     if len(workers.queue) > 0:
#         poller = poll_both
#     else:
#         poller = poll_workers
#     socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
#
#     # Handle worker activity on backend
#     if socks.get(backend) == zmq.POLLIN:
#         # Use worker address for LRU routing
#         frames = backend.recv_multipart()
#         if not frames:
#             break
#
#         address = frames[0]
#         workers.ready(Worker(address))
#
#         # Validate control message, or return reply to client
#         msg = frames[1:]
#         if len(msg) == 1:
#             if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
#                 print("E: Invalid message from worker: %s" % msg)
#         else:
#             frontend.send_multipart(msg)
#
#         # Send heartbeats to idle workers if it's time
#         if time.time() >= heartbeat_at:
#             for worker in workers.queue:
#                 # worker.send_heartbeat(backend)
#                 msg = [worker, PPP_HEARTBEAT]
#                 backend.send_multipart(msg)
#             heartbeat_at = time.time() + HEARTBEAT_INTERVAL
#
#     if socks.get(frontend) == zmq.POLLIN:
#         frames = frontend.recv_multipart()
#         if not frames:
#             break
#
#         frames.insert(0, workers.next())
#         backend.send_multipart(frames)
#
#     workers.purge()
