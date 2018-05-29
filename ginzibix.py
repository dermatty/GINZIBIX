#!/home/stephan/.virtualenvs/nntp/bin/python

import time
import sys
from os.path import expanduser
import configparser
import signal
import multiprocessing as mp
import logging
import logging.handlers
from logging import getLoggerClass, addLevelName, setLoggerClass, NOTSET
import lib
import queue
import zmq
import threading
from threading import Thread

lpref = ""
__version__ = "0.1-alpha"


userhome = expanduser("~")
maindir = userhome + "/.ginzibix/"
dirs = {
    "userhome": userhome,
    "main": maindir,
    "config": maindir + "config/",
    "nzb": maindir + "nzb/",
    "complete": maindir + "complete/",
    "incomplete": maindir + "incomplete/",
    "logs": maindir + "logs/"
}
subdirs = {
    "download": "_downloaded0",
    "renamed": "_renamed0",
    "unpacked": "_unpack0",
    "verififiedrar": "_verifiedrars0"
}


# Signal handler
class SigHandler_Ginzibix:
    def __init__(self, mpp_main, pwdb, logger):
        self.mpp_main = mpp_main
        self.pwdb = pwdb
        self.logger = logger

    def sighandler_ginzibix(self, a, b):
        # wait until main is joined
        if self.mpp_main.pid:
            mpp_main.join()
        # stop pwdb
        self.logger.info(lpref + "closing pewee.db")
        self.pwdb.db_close()
        self.logger.info("ginzibix terminated")
        sys.exit()


# connects to GUI_Connector in main.py and gets data for displaying
class GUI_Poller(Thread):

    def __init__(self, lock, logger, port="36601"):
        Thread.__init__(self)
        self.daemon = True
        self.context = zmq.Context()
        self.host = "127.0.0.1"
        self.port = port
        self.lock = lock
        self.data = None
        self.nzbname = None
        self.delay = 1

        self.socket = self.context.socket(zmq.REQ)
        self.logger = logger
        self.gui_drawer = lib.GUI_Drawer()

    def run(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        socketurl = "tcp://" + self.host + ":" + self.port
        self.socket.connect(socketurl)
        # self.socket.RCVTIMEO = 1000
        while True:
            try:
                self.socket.send_string("REQ")
                (data, pwdb_msg, server_config, threads) = self.socket.recv_pyobj()
                if data == "NOOK":
                    continue
                self.gui_drawer.draw(data, pwdb_msg, server_config, threads)
            except Exception as e:
                self.logger.error("GUI_ConnectorMain: " + str(e))
            time.sleep(self.delay)


# -------------------- main --------------------

if __name__ == '__main__':

    # init config
    cfg = configparser.ConfigParser()
    cfg.read(dirs["config"] + "/ginzibix.config")

    # init logger
    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # init peewee db
    pwdb = lib.PWDB(logger)

    # init download threads
    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    mp_work_queue = mp.Queue()

    # init sighandler
    sh = SigHandler_Ginzibix(None, pwdb, logger)
    signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
    signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

    progstr = "ginzibix 0.1-alpha, client"
    logger.debug(lpref + "Welcome to GINZIBIX " + __version__)

    # start guiconnector
    lock = threading.Lock()
    guipoller = GUI_Poller(lock, logger, port="36601")
    guipoller.start()

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, pwdb, dirs, subdirs, logger, ))
    mpp_main.start()
    sh.mpp_main = mpp_main

    # "main" loop ...
    while True:
        time.sleep(1)

    logger.debug("exiting GNIZIBIX")

    sys.exit()
