import threading
import time
import zmq
import sys
import socket
import heartbeat_server as sh
import zmq.ssh
import queue as q

NBR_CLIENTS = 2
NBR_WORKERS = 50


# new strt
port_fe_hb     = "7001"
# new end
port_client    = "5565"
port_worker    = "5566"
port_monitor   = "5567"
port_heartbeat = "7000"
addr           = "tcp://0.0.0.0 :"

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

    # New start
    fe_hb_f   = ctx.socket(zmq.ROUTER)
    fe_hb_f.bind(addr + port_fe_hb)
    fe_hb_p_f = zmq.Poller()
    fe_hb_p_f.register(fe_hb_f, zmq.POLLIN)
    # New end

    return [client, fe_poller, fe_hb_f, fe_hb_p_f]


def poll_heartbeats(poller, hb_soc):
    heartbeat_at = time.time() + sh.HEARTBEAT_INTERVAL
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

        if time.time() >= heartbeat_at:
            with sh.lock:
                for worker in worker_queue.non_expired:
                    msg = [worker.encode('ascii'), sh.PPP_HEARTBEAT]
                    hb_soc.send_multipart(msg)
                    heartbeat_at = time.time() + sh.HEARTBEAT_INTERVAL

# new start
def poll_fe_hb(fe_poller, fe_hb_soc):
    hb_cli = time.time() + sh.HEARTBEAT_INTERVAL
    while True:
        try:
            events_fe_hb = dict(fe_poller.poll(1000))
        except:
            pass

        if fe_hb_soc in events_fe_hb:
            msg_fe_hb = fe_hb_soc.recv_multipart()
            if msg_fe_hb[-1] == sh.PPP_READY:
                fe_addr = msg_fe_hb[0].decode('ascii')
                client_queue.ready(fe_addr)
            elif msg_fe_hb[-1] == sh.PPP_HEARTBEAT:
                fe_addr = msg_fe_hb[0].decode('ascii')
                client_queue.ready(fe_addr)

        if time.time() >= hb_cli:
            with sh.lock:
                for client in client_queue.queue:
                    msg_send = [client.encode('ascii'), sh.PPP_HEARTBEAT]
                    fe_hb_soc.send_multipart(msg_send)
                    hb_cli = time.time() + sh.HEARTBEAT_INTERVAL
# new end


"""
    Using ROUTER SOCKET
        soc             : zmq Socket
        receiver_addr   : string
        task_msg        : list( [ md, image] )
"""
def send_task_msg_to(soc, receiver_addr, task_msg, client = 0):
    # task_msg_send = []
    soc.send(receiver_addr.encode('ascii'), flags=0 | zmq.SNDMORE)
    soc.send_json(task_msg[0], flags=0 | zmq.SNDMORE)

    if task_msg[0]['option'] == 5 and client == 0:
        soc.send_pyobj(task_msg[1][0], flags=0 | zmq.SNDMORE)
        soc.send_pyobj(task_msg[1][1], flags=0)
    else:
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
    # String
    print(md_json)
    # md = json.loads(md_json)
    client_id   = md_json['client_id']

    if md_json['option'] == 5 and soc != be_worker:
        chunk = list()
        chunk.append(soc.recv_pyobj())
        chunk_overlay = soc.recv_pyobj()
        chunk.append(chunk_overlay)
    else:
        chunk = soc.recv_pyobj()


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
            events = dict(be_poller.poll(10*sh.HEARTBEAT_INTERVAL))
        except zmq.ZMQError:
            print("Error bro")
            # break  # interrupted

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
                try :
                    worker_obj.remove_task()
                    worker_queue.ready(address, add_queue=1)
                except:
                    pass

        if task_comp_msg is not None:
            send_task_msg_to(fe_soc, task_comp_msg[2], task_comp_msg,1)

def send_task_thread():
    while True:
        be_cap = len(worker_queue.queue)
        if be_cap != 0:
            task_msg_thr = expired_proc_task_dict.get()
        # print("Assigned task")

            # New start
            client_id = task_msg_thr[0]['client_id']
            if isinstance(client_id,bytes):
                client_id = client_id.decode('ascii')

            if client_id in client_queue.queue:
            # New end
                try:
                    w_addr, worker_obj = worker_queue.next()
                    worker_obj.update_tasks(task_msg_thr)
                    send_task_msg_to(be_worker, w_addr, task_msg_thr)
                except:
                    pass
        elif expired_proc_task_dict.empty() and be_cap:
            print("Done")


if __name__ == '__main__':
    print("Serving on {} ".format(socket.gethostbyname_ex(socket.gethostname())[-1][-1]))
    # addr = "tcp://" + socket.gethostbyname_ex(socket.gethostname())[-1][-1] + ":"
    addr = "tcp://0.0.0.0:"
    ctx = zmq.Context()

    # Front end clients
    fe_client, fe_poller, fe_hb, fe_hb_poller = set_client(ctx)

    # Backend end workers and monitor msgs
    be_worker, monitor_client, be_poller, hb_soc_be, hb_be_poller, mn_poller = set_worker(ctx)

    # Capacity measurement
    be_cap = 0
    expired_proc_task_dict = q.Queue()

    # Initiate worker queue
    worker_queue = sh.WorkerQueue()

    # New start
    client_queue = sh.ClientQueue()
    # New end

    # Initiate heartbeat
    heartbeat_at = time.time() + sh.HEARTBEAT_INTERVAL

    # Server heartbeat process
    hb_proc_serv = threading.Thread(target=poll_heartbeats, args=(hb_be_poller, hb_soc_be,))
    hb_proc_serv.daemon = True
    hb_proc_serv.start()

    be_comp_proc = threading.Thread(target=comp_task_thread, args=( be_worker, be_poller, fe_client,))
    be_comp_proc.daemon = True
    be_comp_proc.start()

    send_task_be_proc = threading.Thread(target=send_task_thread)
    send_task_be_proc.daemon = True
    send_task_be_proc.start()

    # new starts
    hb_client = threading.Thread(target=poll_fe_hb, args=( fe_hb_poller, fe_hb,))
    hb_client.daemon = True
    hb_client.start()

    # new ends

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
            # if be_cap and len(expired_proc_task_dict) == 0:
                # secondary = zmq.Poller()
                # secondary.register(fe_client, zmq.POLLIN)
                # print("Poller fe")
        while be_cap:
            events = dict(fe_poller.poll(50))
            if fe_client in events:
                # print("Polled inside")
                task_msg = receive_task_from(fe_client)

                # print("Got msg ")
            else:
                break
                # No work, go back to backends
            expired_proc_task_dict.put(task_msg)
            be_cap -= 1

        a = worker_queue.purge()
        for i in a:
            expired_proc_task_dict.put(i)

        client_queue.purge()
