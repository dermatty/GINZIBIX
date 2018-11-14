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
import os
gi.require_version('Gtk', '3.0')
# from gi.repository import GLib, Gtk


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
    def __init__(self, pwdb, mpp_main, mpp_wrapper, logger):
        self.mpp_main = mpp_main
        self.mpp_wrapper = mpp_wrapper
        self.logger = logger
        self.pwdb = pwdb

    def sighandler_ginzibix(self, a, b):
        self.shutdown()

    def shutdown(self):
        # wait until main is joined
        global TERMINATED
        if self.mpp_main:
            if self.mpp_main.pid:
                try:
                    os.kill(self.mpp_main.pid, signal.SIGTERM)
                    self.mpp_main.join()
                except Exception as e:
                    logger.warning(whoami() + str(e))
        self.pwdb.exc("set_exit_goodbye_from_main", [], {})
        self.mpp_wrapper.join()
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

    progstr = "ginzibix 0.1-alpha, client"
    logger.debug(whoami() + "Welcome to GINZIBIX " + __version__)

    # start DB Thread
    mpp_wrapper = mp.Process(target=lib.wrapper_main, args=(cfg, dirs, logger, ))
    mpp_wrapper.start()

    pwdb = lib.PWDBSender()

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, dirs, subdirs, logger, ))
    mpp_main.start()

    # init sighandler
    sh = SigHandler_Ginzibix(pwdb, mpp_main, mpp_wrapper, logger)
    signal.signal(signal.SIGINT, sh.sighandler_ginzibix)
    signal.signal(signal.SIGTERM, sh.sighandler_ginzibix)

    #while not TERMINATED:
    #    time.sleep(0.5)
    #logger.debug(whoami() + "exited!")
    #sys.exit()

    app = lib.Application(dirs, cfg_file, logger)
    exit_status = app.run(sys.argv)

    sh.shutdown()

    sys.exit(exit_status)
