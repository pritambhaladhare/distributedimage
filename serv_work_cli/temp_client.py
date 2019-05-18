import os, fnmatch, imghdr
import random
import socket
import sys
import multiprocessing as mp, threading as th
import time
import zmq
import signal,sys
from img_proc import imageprocessing as ip
import json
import cv2


def asbytes(obj):
    s = str(obj)
    if str is not bytes:
        # Python 3
        s = s.encode('ascii')
    return s


"""
    Sending using DEALER SOCKETS
    sent metadata as json
    send img      as pyobj
"""
def send_imgs_to_server( soc, list_imgs):
    for md, img in list_imgs:
        # print("Sending json {}".format(md))
        soc.send_json( md, flags = 0 | zmq.SNDMORE)
        soc.send_pyobj(img)
        print("Sent everything")


def recv_msg_server( soc):
    md = soc.recv_json()
    # print( type(md))
    img_data  = soc.recv_pyobj()
    # print(type(img_data))
    return [ md, img_data]


""""
img_msgs format :
    [
        metadata format { "client_id" , "img_number", "chunk", "shape", "dtype", "option"}
        image format    { np matrix object}
    ]
"""
def create_msg( cl_id, number, path, option):
    img_msgs = []
    rcv_dict = dict()
    parts                   = 10

    image                   = ip.readImageRGB(path)
    split                   = ip.splitImage(image, parts)
    rcv_dict[number]        = [dict() for i in range(parts)]
    for i in range(len(split)):
        temp_dict               = dict()
        temp_dict["client_id"]  = cl_id.decode('ascii')
        temp_dict["img_number"] = number
        temp_dict["chunk"]      = i
        temp_dict["shape"]      = split[i].shape
        temp_dict["dtype"]      = str(split[i].dtype)
        temp_dict["option"]     = option

        img_msgs.append([temp_dict,split[i]])

    return [img_msgs, rcv_dict[number]]



def client_task(i, img_paths, option):
# def client_task(i):
    """Request-reply client using REQ socket"""
    ctx = zmq.Context()

    client_data = ctx.socket(zmq.DEALER)
    client_data.identity = (u"%s" % (i+6000)).encode('ascii')
    client_data.setsockopt(zmq.IDENTITY, client_data.identity)
    client_data.connect(fe_client_addr)

    client_control = ctx.socket(zmq.PUSH)
    client_control.connect(fe_monitor_addr)

    # img_dict = get_images(img_path)
    img_dict      = dict()
    img_recv_dict = dict()

    poller = zmq.Poller()
    poller.register(client_data, zmq.POLLIN)
    poller.register(client_control, zmq.POLLIN)

    numbr_msgs = 0
    img_number = 0
    for img_path in img_paths:
        img_dict[img_number] = img_path
        task_msg, img_recv_dict[img_number] = create_msg(client_data.identity, img_number, img_path, option)
        numbr_msgs = numbr_msgs + len(task_msg)
        send_imgs_to_server(client_data, task_msg)
        print("Sending {}".format(img_number))
        img_number = img_number + 1

    while True and numbr_msgs:
            # wait max 10 seconds for a reply, then complain
            try:
                events = dict(poller.poll(1000))
            except zmq.ZMQError:
                print("Exited {}".format(client_data.identity))
                return # interrupted

            if events:
                reply   = recv_msg_server( client_data)
                # print('Client recieved string {} from server'.format(reply[0]))
                # assert reply == img_number, "expected %s, got %s" % (img_number, reply)
                numbr_msgs = numbr_msgs - 1
                img_number = reply[0]['img_number']
                chunk      = reply[0]['chunk']
                img_recv_dict[img_number][chunk] = reply[1]
                # client_control.send_string('Client {} received img {} and chunk {} from server'.format(client_data.identity,reply[0]['img_number'], reply[0]['chunk']))
                print('Client {} received img {} and chunk {} from server'.format(client_data.identity,reply[0]['img_number'], reply[0]['chunk']))
            # else:
            #     client_control.send_string(u"E: CLIENT EXIT - lost task %s" % img_number)
            #     return

    for img_number in img_recv_dict.keys():
        img_rgb = ip.readImageRGB(img_dict[img_number])
        img = ip.stitchImage( img_rgb, img_recv_dict[img_number])
        ip.writeimage('output/edge/', 'D_' + img_dict[img_number].split('/')[-1][:-4], img)

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        myserver = asbytes(sys.argv[1])
        fe_client_addr = "tcp://{}:5565".format(myserver.decode('ascii'))
        fe_monitor_addr  = "tcp://{}:5567".format(myserver.decode('ascii'))

        print(fe_monitor_addr)
        print(fe_client_addr)

        host_name = socket.gethostname()
        print("Path to the image(s)")
        option = 1
        while option != 6:
            path, option = ip.main_screen()
            if path != None:
                client_task(os.getpid(), path, option)

        # print("Select filter for the images")

        # client_task(os.getpid())
        # client_task(os.getpid(), path, option)
