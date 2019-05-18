import cv2
import numpy as np
from matplotlib import pyplot as plt
import os

from tkinter.filedialog import askopenfilename, askopenfilenames

# import Tkinter, tkFileDialog


def readImage(path):
    return cv2.imread(path, 0)


def readImageRGB(path):
    return cv2.cvtColor(cv2.imread(path), cv2.COLOR_BGR2RGB)


def medianblur(image):
    # return cv2.cvtColor(cv2.medianBlur(image, 5), cv2.COLOR_BGR2RGB)
    return cv2.medianBlur(image, 5)

def noisereduction(image):
    return cv2.fastNlMeansDenoisingColored(image,None,10,10,7,21)


def edgeDetection(image):
    return cv2.cvtColor(cv2.Canny(image, 60, 100), cv2.COLOR_GRAY2BGR)


def splitImage(image, parts):
    return np.array_split(image, parts)


def stitchImage(image, split):
    # ret = [0] * image.shape[1]
    ret = np.zeros([1, image.shape[1], 3], dtype=int)
    # print(ret)
    for item in split:
        # temp = func(item)
        # ret = ret + temp
        # print("ret shape {}".format(ret.shape))
        # print("temp shape {}".format(temp.shape))
        ret = np.concatenate((ret, item))
    # print(ret.shape)
    return ret[1:]


def stitchmultimage(image, bgimage, split, bgsplit, func):
    # ret = [0] * image.shape[1]
    ret = np.zeros([1, image.shape[1], 3], dtype=int)

    # print(ret)
    i = 0
    for item in split:
        temp = func(item, bgsplit[i])
        i = i+1
        # ret = ret + temp
        # print("ret shape {}".format(ret.shape))
        # print("temp shape {}".format(temp.shape))
        ret = np.concatenate((ret, temp))
    # print(ret.shape)
    return ret


def overlay(img, img2):
    return cv2.addWeighted(img, 0.6, img2, 0.4, 0)


def facedetect(image):
    img = image.copy()
    face_cascade = cv2.CascadeClassifier('cascade/haarcascade_frontalface_default.xml')
    eye_cascade = cv2.CascadeClassifier('cascade/haarcascade_eye.xml')
    # read the image and convert to grayscale format
    # img = cv2.imread('multiface.jpg')
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # calculate coordinates
    faces = face_cascade.detectMultiScale(gray, 1.1, 4)
    for (x, y, w, h) in faces:
        cv2.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 2)
        roi_gray = gray[y:y + h, x:x + w]
        roi_color = img[y:y + h, x:x + w]
        eyes = eye_cascade.detectMultiScale(roi_gray)
        # draw bounding boxes around detected features
        for (ex, ey, ew, eh) in eyes:
            cv2.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (0, 255, 0), 2)
    # plot the image
    return img


def print2images(plt, img1, img2):
    plt.subplot(121), plt.imshow(img1)
    plt.title('First Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(122), plt.imshow(img2)
    plt.title('Second Image'), plt.xticks([]), plt.yticks([])
    return plt


def print3images(plt, img1, img2, img3, *args):
    # print(args)
    plt.subplot(131), plt.imshow(img1, cmap=args[0])
    plt.title('Original Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(132), plt.imshow(img2, cmap=args[0])
    plt.title('Centralised Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(133), plt.imshow(img3, cmap=args[0])
    plt.title('Distributed Image'), plt.xticks([]), plt.yticks([])
    return plt


def print4images(plt, img1, img2, img3, img4, *args):
    # print(args)
    plt.subplot(221), plt.imshow(img1, cmap=args[0])
    plt.title('First Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(222), plt.imshow(img2, cmap=args[0])
    plt.title('Second Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(223), plt.imshow(img3, cmap=args[0])
    plt.title('Centralised Image'), plt.xticks([]), plt.yticks([])
    plt.subplot(224), plt.imshow(img4, cmap=args[0])
    plt.title('Distributed Image'), plt.xticks([]), plt.yticks([])
    return plt


def writeimage0(path, filename, img):
    image = img.copy()
    cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    cv2.imwrite(os.path.join(path, filename)+'.jpg', image)


def writeimage(path, filename, img):
    image = img.copy()
    # print(image.shape)
    cv2.imwrite(os.path.join(path, filename) + '.jpg', image)
    new = readImageRGB(os.path.join(path, filename) + '.jpg')
    cv2.cvtColor(new, cv2.COLOR_BGR2RGB)
    cv2.imwrite(os.path.join(path, filename) + '.jpg', new)
    # writeimage('output/nonlocal/', 'nonlocal distributed', new)


def choiceEdge(filename):
    for file in filename:
        print("Starting process ----")
        print(file)
        edgeImg = readImageRGB(file)
        edge_original = edgeDetection(edgeImg)
        part = 10
        split = splitImage(edgeImg, part)
        edge_distributed = stitchImage(edgeImg, split, edgeDetection)
        fig=plt.figure(figsize=(18, 16), dpi= 80, facecolor='w', edgecolor='k')
        print3images(plt, edgeImg, edge_original, edge_distributed, "gray")
        writeimage('output/edge/', 'C_' + file.split('/')[-1][:-4], edge_original)
        writeimage('output/edge/', 'D_' + file.split('/')[-1][:-4], edge_distributed)
        # writeimage('output/edge/', str(file), edge_original)
        # writeimage('output/edge/', str(file), edge_distributed)

        plt.show()


def choiceMedian(filename):
    for file in filename:
        print("Starting process ----")
        print(file)
        medianImg = readImageRGB(file)
        median_original = medianblur(medianImg)
        part = 10
        split = splitImage(medianImg, part)
        median_distributed = stitchImage(medianImg, split, medianblur)
        fig = plt.figure(figsize=(18, 16), dpi=80, facecolor='w', edgecolor='k')
        print3images(plt, medianImg, median_original, median_distributed, 'gray')
        writeimage('output/median/', 'median centralised', median_original)
        writeimage('output/median/', 'median distributed', median_distributed)
        plt.show()

def replayMenu():
    startover = ""
    startover = input("Would you like to start from the beginning, yes or no? ")
    while startover.lower() != "yes":
        print("Thanks for using our Image Processing Program! Bye! ")
        break
    else:
        main_screen()


def menu():
    print("Select a language so I can translate for you and say 'Good Morning!'")
    print("1. Edge Detection")
    print("2. Noise Reduction using median blur")
    print("3. Noise Reduction using non local means")
    print("4. Image Overlay")
    print("5. Face Detection")
    print("6. Quit\n")

def map_work_to_option( task):
    md  = task[0]
    img = task[1]
    option = md['option']
    if option == 1:
        return [md, edgeDetection(img)]
    elif option == 2:
        return [md, medianblur(img)]

def main_screen():
    choice = 0
    menu()
    choice = eval(input("Please type 1-5 from the menu or press 6 to quit. "))

    if choice == 1:
        filename = askopenfilenames(title='Choose a file')
        return filename, choice
        # choiceEdge(filename)
    elif choice == 2:
        filename = askopenfilename()  # show an "Open" dialog box and return the path to the selected file
        return filename, choice
        # print(filename)
    elif choice == 3:
        filename = askopenfilename()  # show an "Open" dialog box and return the path to the selected file
        return filename, choice
    elif choice == 4:
        filename = askopenfilename()  # show an "Open" dialog box and return the path to the selected file
        return filename, choice
    elif choice == 5:
        filename = askopenfilename()  # show an "Open" dialog box and return the path to the selected file
        return filename, choice
    elif choice == 6:
        print("Good Bye! ")
        return None, choice
    else:
        print("That is not a valid selection, please type the correct number\n")
        main_screen()
    while choice == 6:
        break
    else:
        replayMenu()

# main_screen()
""" Edge Detection call """

#
""" Noise reduction using Median Blur Call """



#
# """ Noise reduction using Non Local Means Denoising """
#
# noiseImg = readImageRGB("images/noise2.jpg")
# noise_original = noisereduction(noiseImg)
# part = 10
# split = splitImage(noiseImg, part)
# noise_distributed = stitchImage(noiseImg, split, noisereduction)
# fig = plt.figure(figsize=(18, 16), dpi= 80, facecolor='w', edgecolor='k')
# print3images(plt, noiseImg, noise_original, noise_distributed, 'gray')
# writeimage('output/nonlocal/', 'nonlocal centralised', noise_original)
# writeimage('output/nonlocal/', 'nonlocal distributed', noise_distributed)
# # plt.show()
#
#
# """ Overlay Images """
#
# frontImage = readImageRGB("images/fall.jpg")
# bgImage = readImageRGB("images/ocean.jpg")
# # print(img2.shape)
# overlayImage = overlay(frontImage, bgImage)
# split = splitImage(frontImage, part)
# bgsplit = splitImage(bgImage, part)
# overlay_distributed = stitchmultimage(frontImage, bgImage, split, bgsplit,overlay)
# fig=plt.figure(figsize=(18, 16), dpi= 80, facecolor='w', edgecolor='k')
# print4images(plt, frontImage, bgImage, overlayImage, overlay_distributed,'gray')
# writeimage('output/overlay/', 'overlay original', overlayImage)
# writeimage('output/overlay/', 'overlay distributed', overlay_distributed)
# # plt.show()
#
#
# """ Face Detection """
#
# faceImg = readImageRGB("images/faced.jpg")
# faceImg_original = facedetect(faceImg)
# part = 3
# split = splitImage(faceImg, part)
# faceImg_distributed = stitchImage(faceImg, split, facedetect)
# fig=plt.figure(figsize=(18, 16), dpi= 80, facecolor='w', edgecolor='k')
# print3images(plt, faceImg, faceImg_original, faceImg_distributed, 'gray')
# writeimage('output/face/', 'fd original', faceImg_original)
# writeimage('output/face/', 'fd distributed', faceImg_distributed)
# plt.show()
#
# print("Finished")
#
#

