import os, fnmatch, imghdr
import random
import socket
import sys
import threading
import time
import zmq
import signal,sys


def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


def get_images(img_path):
    img_list = []
    pattern = set(['rgb', 'pgm', 'ppm', 'jpeg', 'png', 'webp'])
    if os.path.isdir(img_path):
        for files in os.listdir(img_path):
            if imghdr.what(img_path) in pattern:
                img_list.append(files)
    elif os.path.isfile(img_path):
        if imghdr.what(img_path) in pattern:
            img_list.append(img_path)
    else:
        print("Not a file")
    return img_list


# def client_task(i, img_path):
def client_task(i):
    """Request-reply client using REQ socket"""
    ctx = zmq.Context()

    client_data = ctx.socket(zmq.REQ)
    client_data.identity = (u"%s" % (i+6000)).encode('ascii')
    client_data.connect(fe_client_addr)

    client_control = ctx.socket(zmq.PUSH)
    client_control.connect(fe_monitor_addr)

    # img_list = get_images(img_path)

    poller = zmq.Poller()
    poller.register(client_data, zmq.POLLIN)
    poller.register(client_control, zmq.POLLIN)

    while True:
        time.sleep(random.randint(0, 5))
        for _ in range(random.randint(0, 15)):
            # send request with random hex ID
            task_id = u"%04X" % random.randint(0, 10)
            print("Sending {}".format(task_id))
            client_data.send_string(task_id)
            # wait max 10 seconds for a reply, then complain
            try:
                events = dict(poller.poll(10000))
            except zmq.ZMQError:
                print("Exited {}".format(client_data.identity))
                return # interrupted

            if events:
                reply = client_data.recv_string()
                print('Client recieved string {} from server'.format(reply))
                assert reply == task_id, "expected %s, got %s" % (task_id, reply)
                client_control.send_string('Client {} received string {} from server'.format(client_data.identity,reply))
            else:
                client_control.send_string(u"E: CLIENT EXIT - lost task %s" % task_id)
                return


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        myserver = asbytes(sys.argv[1])
        fe_client_addr = "tcp://{}:5565".format(myserver.decode('ascii'))
        fe_monitor_addr  = "tcp://{}:5567".format(myserver.decode('ascii'))

        print(fe_monitor_addr)
        print(fe_client_addr)

        host_name = socket.gethostname()
        #path = sys.argv[2]
        client_task(os.getpid())
        # client_task(os.getpid(), path)
