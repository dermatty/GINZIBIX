#!/home/stephan/.virtualenvs/nntp/bin/python

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
import gi
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


# -------------------- main --------------------

if __name__ == '__main__':

    # init config
    cfg = configparser.ConfigParser()
    cfg.read(dirs["config"] + "/ginzibix.config")

    # init logger
    logger = logging.getLogger("ginzibix")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
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
    nzbinqueue = mp.Queue()

    # init sighandler
    sh = SigHandler_Ginzibix(None, pwdb, logger)
    signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
    signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

    progstr = "ginzibix 0.1-alpha, client"
    logger.debug(lpref + "Welcome to GINZIBIX " + __version__)

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, pwdb, dirs, subdirs, nzbinqueue, logger, ))
    mpp_main.start()
    sh.mpp_main = mpp_main

    # "main" loop ...
    lib.app_main(nzbinqueue, mpp_main, logger)
    Gtk.main()
