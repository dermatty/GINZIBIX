#!/home/stephan/.virtualenvs/nntp/bin/python

import configparser
import signal
import multiprocessing as mp
import logging
import logging.handlers
import lib
import queue
import gi
import sys
import inspect
gi.require_version('Gtk', '3.0')
from gi.repository import GLib, Gtk
import time


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


# !! remove if gtkgui is activated ###
TERMINATED = False

__version__ = "0.1-alpha"


# Signal handler
class SigHandler_Ginzibix:
    def __init__(self, mpp_main, mpp_wrapper, pwdb, logger):
        self.mpp_main = mpp_main
        self.mpp_wrapper = mpp_wrapper
        self.pwdb = pwdb
        self.logger = logger

    def sighandler_ginzibix(self, a, b):
        # wait until main is joined
        global TERMINATED
        if self.mpp_main:
            if self.mpp_main.pid:
                # !! remove if called from gtkgui !!!
                # os.kill(self.mpp_main.pid, signal.SIGKILL)
                mpp_main.join()
                print("main joined!")
        if self.mpp_wrapper:
            if self.mpp_wrapper.pid:
                # !! remove if called from gtkgui !!!
                # os.kill(self.mpp_main.pid, signal.SIGKILL)
                mpp_wrapper.join()
                print("peewee_wrapper joined!")
        TERMINATED = True


# -------------------- main --------------------

if __name__ == '__main__':
    # dirs
    userhome, maindir, dirs, subdirs = lib.make_dirs()

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

    logger.debug(whoami() + "starting ...")

    # init download threads
    articlequeue = queue.LifoQueue()
    resultqueue = queue.Queue()
    mp_work_queue = mp.Queue()

    # init sighandler
    sh = SigHandler_Ginzibix(None, None, None, logger)
    signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
    signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

    progstr = "ginzibix 0.1-alpha, client"
    logger.debug(whoami() + "Welcome to GINZIBIX " + __version__)

    # start DB Thread
    mpp_wrapper = mp.Process(target=lib.wrapper_main, args=(cfg, dirs, logger, ))
    mpp_wrapper.start()
    sh.mpp_wrapper = mpp_wrapper

    pwdb = lib.PWDBSender()

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, dirs, subdirs, logger, ))
    mpp_main.start()
    sh.mpp_main = mpp_main

    #while not TERMINATED:
    #    time.sleep(0.5)
    #logger.debug(whoami() + "exited!")

    app = lib.Application(mpp_main, dirs, cfg_file, logger)
    exit_status = app.run(sys.argv)
    sys.exit(exit_status)
