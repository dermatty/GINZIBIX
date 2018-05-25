import os
import time
import multiprocessing as mp
import signal
import sys


def sighandler(a, b):
    print("main got signal", a)
    # os.kill(os.getpid(), signal.SIGUSR1)
    # os.kill(mpp_ginzimain.pid, signal.SIGTERM)
    mpp_ginzimain.join()
    print("**** main done ****")
    sys.exit()


def ginzi_main():

    def sighandler1(a, b):
        print("ginzi_main got signal", a)
        if mpp_unrar.pid:
            # os.kill(mpp_unrar.pid, signal.SIGTERM)
            mpp_unrar.join()
        print("**** ginzi main done ****")
        sys.exit()

    #signal.signal(signal.SIGINT, signal.SIG_IGN)
    #signal.signal(signal.SIGTERM, signal.SIG_IGN)
    mpp_unrar = mp.Process(target=unrar)
    mpp_unrar.start()
    signal.signal(signal.SIGINT, sighandler1)
    signal.signal(signal.SIGTERM, sighandler1)
    while True:
        print("ginzibix > ginzi main")
        time.sleep(1)
    print("ginzi_main terminated")


def unrar():

    def sighandler2(a, b):
        print("unrar got signal", a)
        print("**** unrar done ****")
        sys.exit()

    signal.signal(signal.SIGINT, sighandler2)
    signal.signal(signal.SIGTERM, sighandler2)
    while True:
        print("ginzibix > ginzi main > unrar")
        time.sleep(0.5)
    print("unrar terminated!")


#signal.signal(signal.SIGINT, signal.SIG_IGN)
#signal.signal(signal.SIGTERM, signal.SIG_IGN)
mpp_ginzimain = mp.Process(target=ginzi_main)
mpp_ginzimain.start()
signal.signal(signal.SIGINT, sighandler)
signal.signal(signal.SIGTERM, sighandler)
while True:
    print("main running")
    time.sleep(2)
