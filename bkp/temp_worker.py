import os
import multiprocessing as mp, threading as th
import time
import zmq, zmq.ssh
import sys
import socket
import heartbeat_worker as hw
import imageprocessing as ip
import time

port_task       = "5566"
port_hb         = "7000"
worker_task_soc = None
task_poller     = None
pool            = None
liveness        = hw.HEARTBEAT_LIVENESS


def task_thread(_msg_queue, _done_queue, _worker_task_soc):
    while True:
        if not _msg_queue.empty():
            work = _msg_queue.get()
            # do work
            # push the done work into done_queue
            _done_queue.put(work)
            _worker_task_soc.send_multipart(work)


def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


"""
    Task socket      : DEALER SOCKET
    Heartbeat socket : DEALER SOCKET
"""
def worker_soc_create(ctx, be_worker_addr, task, hb):
    # for getting data from server
    worker_soc_task = ctx.socket(zmq.DEALER)
    worker_soc_task.identity = ("%s" % (12000 + os.getpid())).encode('ascii')
    worker_soc_task.setsockopt(zmq.IDENTITY, worker_soc_task.identity)
    worker_soc_task.connect(be_worker_addr+task)
    task_poller = zmq.Poller()
    task_poller.register(worker_soc_task, zmq.POLLIN)

    # for maintaining heartbeat
    worker_soc_heartbeat = ctx.socket(zmq.DEALER)
    worker_soc_heartbeat.identity = ("%s" % (12000 + os.getpid())).encode('ascii')
    worker_soc_heartbeat.setsockopt(zmq.IDENTITY, worker_soc_heartbeat.identity)
    worker_soc_heartbeat.connect(be_worker_addr + hb)
    heartbeat_poller = zmq.Poller()
    heartbeat_poller.register(worker_soc_heartbeat, zmq.POLLIN)

    return [worker_soc_task, task_poller, worker_soc_heartbeat, heartbeat_poller]


def manage_heartbeat(worker_hb_soc, worker_hb_poller):
    # Tell broker we're ready for work
    interval = 2
    global liveness
    worker_hb_soc.send(hw.PPP_READY)
    heartbeat_at = time.time() + hw.HEARTBEAT_INTERVAL
    while True:
        if time.time() > heartbeat_at and liveness > 0:
            heartbeat_at = time.time() + hw.HEARTBEAT_INTERVAL
            # print("I: Worker heartbeat")
            # worker_task_soc.send(hw.PPP_HEARTBEAT)
            worker_hb_soc.send(hw.PPP_HEARTBEAT)
            interval = 2
        elif liveness < 0:
            heartbeat_at = time.time() + hw.HEARTBEAT_INTERVAL
            print("Sent ppp ready")
            worker_hb_soc.send(hw.PPP_READY)

        try:
            hb_server = dict(worker_hb_poller.poll(hw.HEARTBEAT_INTERVAL*1000))
        except zmq.ZMQError:
            print("Exited {}".format(worker_hb_soc.identity))

        if worker_hb_soc in hb_server:
            msg = worker_hb_soc.recv_multipart()
            if len(msg) == 1 and msg[0] == hw.PPP_HEARTBEAT:
                # print("I: Queue heartbeat")
                liveness = hw.HEARTBEAT_LIVENESS
        else:
            if liveness <= 0:
                print("Server down, will ping again in {}".format(interval))
                if interval <= 8:
                    interval = interval*2
                time.sleep(interval)
                worker_hb_soc.send(hw.PPP_READY)
            else:
                liveness = liveness - 1

"""
   Recieve format   :
        md          : metadata
        img_data    : np array 
"""
def receive_task( soc):
    # We do need to explicitly get the current worker id from the msg frame
    md = soc.recv_json()
    # print( type(md))
    # print(type(img_data))

    if md['option'] == 5:
        img_data = list()
        img_data.append(soc.recv_pyobj())
        overlay = soc.recv_pyobj()
        img_data.append(overlay)
    else:
        img_data = soc.recv_pyobj()

    return [ md, img_data]


"""
    md      : JSON
    img     : np.array
    Send format     :
        md      :  JSON
        img     :  np.array
"""
def send_complete_task( soc, md, img):
    soc.send_json(md, flags=0 | zmq.SNDMORE)
    # print(type(img))
    soc.send_pyobj(img)


def worker_task_dq():
    while True:
        # try:
        count = mp.cpu_count() - 1
        task_list = []
        for i in range(count):
            if not proc_queue.empty():
                # print("Started proc")
                task_list.append( proc_queue.get())
            else:
                break
        # if len(task_list) != 0:
        #     print("{} tl".format(len(task_list)))
        #     print("{} tl0".format(len(task_list[0])))
        #     print("{} tl01".format(len(task_list[0][1])))
        #     input()
        results = pool.map_async(ip.map_work_to_option, task_list).get()

        for i in results:
            # print("Ended proc")
            # print("Started sending")
            send_queue.put(i)
            print("Worker {} done with {}".format(worker_task_soc.identity, i[0]['img_number'],i[0]['chunk']))
        # except Exception as e:
        #     print(" {}".format(e))
        # send_complete_task(worker_task_soc, task[0], task[1])

def worker_task_enq( ):
    """Worker using REQ socket to do LRU routing"""
    # Process messages as they arrive
    while True:
        try:
            # print("Polling")
            socks = dict(task_poller.poll(hw.HEARTBEAT_INTERVAL * 10))
            # print(socks)
        except zmq.ZMQError:
            # interrupted
            print("Exited {}".format(worker_task_soc.identity))
            return

        if socks.get(worker_task_soc) == zmq.POLLIN:
            recv_msg = receive_task( worker_task_soc)
            # Put recieved msg into the queue
            proc_queue.put(recv_msg)

            # if len(recv_msg) == 2:
            #     print("I: Normal reply")
            #     # worker_soc.send_multipart(msg)
            #     liveness = hw.HEARTBEAT_LIVENESS
            #     # time.sleep(1)  # Do some heavy work
            # else:
            #     print("E: Invalid message: %s" % recv_msg)
            # # recv_msg[0] = json.loads(recv_msg[0])
            # print("Worker {} done with {}".format(worker_task_soc.identity, recv_msg[0]))
            # send_complete_task( worker_soc, recv_msg[0], recv_msg[1])
            # worker_task_soc.send_multipart(msg)
        if not send_queue.empty() and liveness > 0:
            item = send_queue.get()
            send_complete_task( worker_task_soc, item[0], item[1])


if __name__ == '__main__':
    # mp.freeze_support()
    if len(sys.argv) >= 2:
        myserver = asbytes(sys.argv[1])
        print(myserver)

        be_worker_addr = "tcp://{}:".format(myserver.decode('ascii'))

        # Create socket for queueing the image msgs
        ctx = zmq.Context()
        worker_task_soc, task_poller, hb_soc, hb_poll = worker_soc_create(ctx, be_worker_addr, port_task, port_hb)

        host_name   = socket.gethostname()
        proc_queue  = mp.Queue()
        send_queue  = mp.Queue()
        pool        = mp.Pool()

        print("Heartbeat thread running")
        hb_proc = th.Thread(target=manage_heartbeat, args=(hb_soc, hb_poll,))
        hb_proc.daemon = True
        hb_proc.start()

        print("Worker task dequeue proc running")
        worker_task_dq_proc = th.Thread(target=worker_task_dq)
        worker_task_dq_proc.daemon = True
        worker_task_dq_proc.start()

        worker_task_enq()
        # print("Worker task enqueue proc running")
        # worker_task_enq_proc = th.Thread(target=worker_task_enq)
        # worker_task_enq_proc.daemon = True
        # worker_task_enq_proc.start()
        hb_proc.join()
        worker_task_dq_proc.join()

        # worker_task(proc_queue, done_queue, worker_task_soc, poller)
