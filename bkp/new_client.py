import os
import socket
import zmq
import sys
import imageprocessing as ip
import threading as th
import cv2


global client_data
global client_hb

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
    for data in list_imgs:
        # print("Sending json {}".format(md))
        md  = data[0]
        img = None
        img_over = None

        # Check if overlay image
        if len(data) == 3 :
            img      = data[1]
            img_over = data[2]
            print("{} {}".format(img.shape,img_over.shape))
        else:
            img      = data[1]

        # Send the metadata
        soc.send_json( md, flags = 0 | zmq.SNDMORE)

        if len(data) == 3:
            # Send overlay and the image
            soc.send_pyobj(img, flags=0| zmq.SNDMORE)
            soc.send_pyobj(img_over)
        else:
            # Send only the image
            soc.send_pyobj(img)

        # print("Sent everything")


def recv_msg_server( soc):
    md = soc.recv_json()
    # print( type(md))
    img_data  = soc.recv_pyobj()
    # print(type(img_data))
    return [ md, img_data]



"""
    4k    min pixels    =  3840 x 2160 = 8294400
    1080p min pixels    =  1920 x 1080 = 2073600
    720p  min pixels    =  1280 x 720  = 921600
"""
def break_image(option, image_shape):
    tot_pix = image_shape[0] * image_shape[1]
    print(image_shape)
    # print(tot_pix)
    if option != 4:
        if tot_pix >= 8294400:
            parts = int(image_shape[1] * image_shape[0] / 500000)
        elif tot_pix >= 2073600 and tot_pix < 8294400:
            parts = int(image_shape[1] * image_shape[0] / 90000)
        elif tot_pix >= 921600 and tot_pix < 2073600:
            parts = int(image_shape[1] * image_shape[0] / 115200)
        else:
            parts = 20
    else:
        parts = 7
    return parts


""""
img_msgs format :
    [
        metadata format { "client_id" , "img_number", "chunk", "shape", "dtype", "option"}
        image format    { np matrix object}
    ]
    
img_msgs format for overlay:
    [
        metadata format { "client_id" , "img_number", "chunk", "shape", "dtype", "option"}
        image format    { np matrix object}
        overlay format  { np matrix object}
    ]
"""
def create_msg( cl_id, number, path, option, overlay_img = None):
    img_msgs = []
    rcv_dict = dict()
    parts_overlay           = 0
    split_overlay           = None
    image                   = ip.readImageRGB(path)

    # break the image into parts
    parts = break_image(option, image.shape)
    req_shape = (image.shape[1], image.shape[0])

    # check if we have overlay option selected
    if overlay_img != None:
        img_overlay   = ip.readImageRGB(overlay_img)
        img_overlay    = cv2.resize(img_overlay, req_shape, interpolation = cv2.INTER_AREA)
        img_o_shape   = img_overlay.shape
        print("{}  {}".format(img_o_shape, req_shape))
        parts_overlay = break_image( option, img_o_shape)
        split_overlay = ip.splitImage( img_overlay, parts_overlay)

    # Get the list of chunks into split list
    split                   = ip.splitImage(image, parts)

    # Initialize a dictionary for listing received processed images
    rcv_dict[number]        = [dict() for i in range(parts)]

    # Create metadata for each image chunk
    for i in range(len(split)):
        temp_dict               = dict()
        temp_dict["client_id"]  = cl_id.decode('ascii')
        temp_dict["img_number"] = number
        temp_dict["chunk"]      = i
        temp_dict["shape"]      = split[i].shape
        temp_dict["dtype"]      = str(split[i].dtype)
        temp_dict["option"]     = option
        if split_overlay != None:
            # print("{} Shape appended".format(split_overlay[i].shape))
            # print("{} Shape required".format(split[i].shape))
            img_msgs.append([ temp_dict, split[i], split_overlay[i]])
        else:
            img_msgs.append([temp_dict, split[i]])

    # Each element of img_msgs has size 3 if overlay option is selected
    # if    overlay return type is [ [ metadata, image_chunk, overlay_chunk], ... ]
    # else          return type is [ [ metadata, image_chunk], ... ]
    return [img_msgs, rcv_dict[number]]


def client_task(i, img_paths, option, path_overlay = None):
# def client_task(i):
    """Request-reply client using DEALER socket"""

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
        task_msg, img_recv_dict[img_number] = create_msg(client_data.identity, img_number, img_path, option, path_overlay)
        numbr_msgs = numbr_msgs + len(task_msg)

        send_imgs_to_server(client_data, task_msg)
        print("Sending {}".format(img_number))
        img_number = img_number + 1

    while True and numbr_msgs:
            # wait max 10 seconds for a reply, then complain
            try:
                events = dict(poller.poll(10))
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

    print("Starting Stitching")
    for img_number in img_recv_dict.keys():
        img_rgb = ip.readImageRGB(img_dict[img_number])
        img = ip.stitchImage( img_rgb, img_recv_dict[img_number])
        ip.writeimage(ip.get_folder(option), 'D_' + img_dict[img_number].split('/')[-1][:-4], img)
        # new = readImageRGB(os.path.join(path, filename) + '.jpg')
        # cv2.cvtColor(new, cv2.COLOR_BGR2RGB)
        # cv2.imwrite(os.path.join(path, filename) + '.jpg', new)
    print("End Stitching")


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        myserver = asbytes(sys.argv[1])
        fe_client_addr = "tcp://{}:5565".format(myserver.decode('ascii'))
        fe_monitor_addr  = "tcp://{}:5567".format(myserver.decode('ascii'))

        ctx = zmq.Context()

        client_data = ctx.socket(zmq.DEALER)
        client_data.identity = (u"%s" % (os.getpid() + 6000)).encode('ascii')
        client_data.setsockopt(zmq.IDENTITY, client_data.identity)
        client_data.connect(fe_client_addr)

        client_control = ctx.socket(zmq.PUSH)
        client_control.connect(fe_monitor_addr)

        print(fe_monitor_addr)
        print(fe_client_addr)

        host_name = socket.gethostname()
        print("Path to the image(s)")
        option = 1

        while option < 6:
            path, option = ip.main_screen(7)

            # handling normal options
            if option != 5 and path != None:
                if path != None:
                    client_task(os.getpid(), path, option,)
                    # th.Thread(target=client_task,args=(os.getpid(), path, option,)).start()

            # for handling overlay images
            elif path != None:
                path_all_imgs, option = ip.main_screen(20)
                client_task(os.getpid(), path_all_imgs, option, path)
            # for handling none paths
            else:
                print("Path not selected, please select images after selecting option")
