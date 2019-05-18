import threading
import time
import zmq
import sys
import socket
from serv_work_cli import heartbeat_server as sh
import json
import numpy as np

NBR_CLIENTS = 2
NBR_WORKERS = 50

port_client    = "5565"
port_worker    = "5566"
port_monitor   = "5567"
port_heartbeat = "7000"
addr           = "tcp://192.168.1.11:"

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)


def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


def set_worker(ctx):

    # task socket
    be_worker = ctx.socket(zmq.ROUTER)
    be_worker.bind(addr + port_worker)
    # monitor socket
    monitor_client = ctx.socket(zmq.PULL)
    monitor_client.bind(addr + port_monitor)

    # hb_worker socket
    hb_worker = ctx.socket(zmq.ROUTER)
    hb_worker.bind(addr + port_heartbeat)

    print("Polling initialization on worker side")
    be_poller = zmq.Poller()
    hb_poller = zmq.Poller()
    mn_poller = zmq.Poller()

    be_poller.register(be_worker, zmq.POLLIN)
    mn_poller.register(monitor_client, zmq.POLLIN)
    hb_poller.register(hb_worker, zmq.POLLIN)

    return [be_worker, monitor_client, be_poller, hb_worker, hb_poller, mn_poller]


def set_client(ctx):
    # client socket
    client = ctx.socket(zmq.ROUTER)
    client.bind(addr + port_client)

    print("Polling initialization on client side")
    fe_poller = zmq.Poller()
    fe_poller.register(client, zmq.POLLIN)

    return [client, fe_poller]


def poll_heartbeats(poller, hb_soc):
    while True:
        try:
            events_hb = dict(poller.poll(1000))
        except zmq.ZMQError:
            print("Error bro")

        if hb_soc in events_hb:
            # get ready msg from the worker and add worker address to the address pool
            msg_hb = hb_soc.recv_multipart()
            # If it's READY, don't route the message any further
            if msg_hb[-1] == sh.PPP_READY:
                address_hb = msg_hb[0].decode('ascii')
                # be_cap = be_cap + 1
                worker_queue.ready(address_hb)
                print("Hello recieved from poll_func {}".format(address_hb))
            elif msg_hb[-1] == sh.PPP_HEARTBEAT:
                address_hb = msg_hb[0].decode('ascii')
                worker_queue.ready(address_hb)
                # print("Hearbeat recieved from poll_func {}".format(address_hb.decode('ascii')))


"""
    Using ROUTER SOCKET
        soc             : zmq Socket
        receiver_addr   : string
        task_msg        : list( [ md, image] )
"""
def send_task_msg_to(soc, receiver_addr, task_msg):
    # task_msg_send = []
    soc.send(receiver_addr.encode('ascii'), flags=0 | zmq.SNDMORE)
    soc.send_json(task_msg[0], flags=0 | zmq.SNDMORE)
    soc.send_pyobj(task_msg[1], flags=0)


"""
    From DEALER SOCKET
    Receive format
        client_id
        metadata in string format
        chunk of image
"""
def receive_task_from( soc):
    sender_id   = soc.recv()
    md_json      = soc.recv_json()
    chunk       = soc.recv_pyobj()
    # String
    print(md_json)
    # md = json.loads(md_json)
    client_id   = md_json['client_id']
    # print(type(sender_id))
    # print(type(md_json))
    # print(type(chunk))
    return [ md_json, chunk, client_id, sender_id]


def monitor_msg_thread( poller, soc):
    while True:
        try:
            events = dict(poller.poll(1000))
        except zmq.ZMQError:
            print("Error bro")
            break  # interrupted

        # handle monitor message
        if soc in events:
            a = soc.recv_string()


def comp_task_thread( be_soc, be_poller, fe_soc):
    while True:
        be_cap = len(worker_queue.queue)
        try:
            events = dict(be_poller.poll(10))
        except zmq.ZMQError:
            print("Error bro")
            break  # interrupted

        previous = be_cap
        # Handle reply from local worker
        task_comp_msg = None
        # GET WORKER ADDRESS IF A WORKER IS FREE AND NUMBER OF WORKERS IS LESS THAN MAX

        if be_soc in events and be_cap != NBR_WORKERS:
            # get ready msg from the worker and add worker address to the address pool
            print("Receiving completed msg")
            task_comp_msg = receive_task_from(be_soc)
            # print("Comp msg {} to {}".format(task_comp_msg[0], task_comp_msg[2]))
            address = task_comp_msg[3].decode('ascii')

            worker_obj = worker_queue.worker_at(address)
            if worker_obj != None:
                worker_obj.remove_task()
                worker_queue.ready(address, add_queue=1)

        if task_comp_msg is not None:
            proc = threading.Thread(target=send_task_msg_to, args=(fe_soc, task_comp_msg[2], task_comp_msg,))
            proc.daemon = True
            proc.start()


if __name__ == '__main__':
    print("Serving on {} ".format(socket.gethostbyname_ex(socket.gethostname())[-1][-1]))
    addr = "tcp://" + socket.gethostbyname_ex(socket.gethostname())[-1][-1] + ":"
    ctx = zmq.Context()

    # Front end clients
    fe_client, fe_poller = set_client(ctx)

    # Backend end workers and monitor msgs
    be_worker, monitor_client, be_poller, hb_soc_be, hb_be_poller, mn_poller = set_worker(ctx)

    # Capacity measurement
    be_cap = 0
    expired_proc_task_dict = dict()

    # Initiate worker queue
    worker_queue = sh.WorkerQueue()

    # Initiate heartbeat
    heartbeat_at = time.time() + sh.HEARTBEAT_INTERVAL

    # Server heartbeat process
    hb_proc_serv = threading.Thread(target=poll_heartbeats, args=(hb_be_poller, hb_soc_be,))
    hb_proc_serv.daemon = True
    hb_proc_serv.start()

    be_comp_proc = threading.Thread(target=comp_task_thread, args=( be_worker, be_poller, fe_client,))
    be_comp_proc.daemon = True
    be_comp_proc.start()

    # monitor_msg_proc = threading.Thread(target=monitor_msg_thread, args=(mn_poller, monitor_client, ))
    # monitor_msg_proc.daemon = True
    # monitor_msg_proc.start()

    while True:
        # POLL FROM WORKERS TO CHECK WHO ARE DONE
        # print(workers)

        # Now route as many clients requests as we can handle
        # - If we have local capacity we poll both localfe
        # print("In loop capacity {}".format(len(worker_queue.queue)))
        be_cap = len(worker_queue.queue)
        while be_cap:
            if be_cap and len(expired_proc_task_dict) == 0:
                # secondary = zmq.Poller()
                # secondary.register(fe_client, zmq.POLLIN)
                # print("Poller fe")
                events = dict(fe_poller.poll(10))

                if fe_client in events:
                    print("Polled inside")
                    task_msg = receive_task_from( fe_client)
                    print("Got msg ")
                else:
                    break  # No work, go back to backends

                # t = str(workers.pop(0))
                w_addr, worker_obj = worker_queue.next()
                worker_obj.update_tasks(task_msg)

                # Send msg thread
                send_task_msg_to(be_worker, w_addr, task_msg)

                be_cap -= 1
            elif be_cap and len(expired_proc_task_dict)!= 0:
                w_addr, worker_obj = worker_queue.next()
                task_msg = expired_proc_task_dict.pop(0)
                worker_obj.update_tasks(task_msg)

                # Send msg thread
                send_task_msg_to(be_worker, w_addr, task_msg)
                be_cap -= 1
            # be_cap = len(worker_queue.queue)
            # expired_proc_task_dict = worker_queue.purge()

        if time.time() >= heartbeat_at:
            # for worker in worker_queue.queue:
            #     msg_hb_serv = [worker.encode('ascii'), sh.PPP_HEARTBEAT]
            #     # print("Sent heart beat")
            #
            #     hb_soc_be.send_multipart(msg_hb_serv)
            #
            #     # be_worker.send_multipart(msg)
            #     # be_worker.send_string(worker, flags=0 | zmq.SENDMORE)
            #     # be_worker.send_string(sh.PPP_HEARTBEAT)
            heartbeat_at = time.time() + sh.HEARTBEAT_INTERVAL
        expired_proc_task_dict = worker_queue.purge()

