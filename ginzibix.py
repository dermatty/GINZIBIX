#!/home/stephan/.virtualenvs/nntp/bin/python

import sys
import os
from os.path import expanduser
import configparser
import signal
import multiprocessing as mp
import logging
import logging.handlers
import lib
import queue
import gi
import time
gi.require_version('Gtk', '3.0')
from gi.repository import GLib, Gtk


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
    def __init__(self, mpp_main, mpp_wrapper, pwdb, logger):
        self.mpp_main = mpp_main
        self.mpp_wrapper = mpp_wrapper
        self.pwdb = pwdb
        self.logger = logger

    def sighandler_ginzibix(self, a, b):
        # wait until main is joined
        if self.mpp_main:
            if self.mpp_main.pid:
                mpp_main.join()
        if self.mpp_wrapper:
            if self.mpp_wrapper.pid:
                mpp_wrapper.join()

        # stop pwdb
        self.logger.info("ginzibix terminated")
        sys.exit()


# -------------------- main --------------------

if __name__ == '__main__':

    # init config
    cfg_file = dirs["config"] + "/ginzibix.config"
    cfg = configparser.ConfigParser()
    cfg.read(cfg_file)

    # init logger
    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fh = logging.FileHandler("/home/stephan/.ginzibix/logs/ginzibix.log", mode="w")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # init peewee db
    # pwdb = lib.PWDB(cfg, dirs, logger)

    # init download threads
    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    mp_work_queue = mp.Queue()

    # init sighandler
    sh = SigHandler_Ginzibix(None, None, None, logger)
    signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
    signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

    progstr = "ginzibix 0.1-alpha, client"
    logger.debug(lpref + "Welcome to GINZIBIX " + __version__)

    # start DB Thread
    mpp_wrapper = mp.Process(target=lib.wrapper_main, args=(cfg, dirs, logger, ))
    mpp_wrapper.start()
    sh.mpp_wrapper = mpp_wrapper

    pwdb = lib.PWDBSender(cfg)

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, dirs, subdirs, logger, ))
    mpp_main.start()
    sh.mpp_main = mpp_main

    while True:
        time.sleep(0.5)

    # app = lib.Application(mpp_main, mpp_wrapper, dirs, cfg_file, logger)
    # exit_status = app.run(sys.argv)
    # sys.exit(exit_status)
