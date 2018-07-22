from threading import Thread
import threading
from .gpeewee import PWDB
import time
import zmq
import inspect
import signal

TERMINATED = False


def whoami():
    outer_func_name = str(inspect.getouterframes(inspect.currentframe())[1].function)
    outer_func_linenr = str(inspect.currentframe().f_back.f_lineno)
    lpref = __name__.split("lib.")[-1] + " - "
    return lpref + outer_func_name + " / #" + outer_func_linenr + ": "


class Sighandler_gpww:
    def __init__(self, logger):
        self.logger = logger

    def sighandler_gpww(self, a, b):
        global TERMINATED
        self.logger.info(whoami() + "terminating ...")
        TERMINATED = True


class PwWrapperThread(Thread):

    def __init__(self, cfg, dirs, logger):
        Thread.__init__(self)
        self.daemon = True
        self.cfg = cfg
        self.logger = logger
        self.dirs = dirs
        self.pwdb = PWDB(self.cfg, self.dirs, self.logger)
        try:
            self.port = self.cfg["OPTIONS"]["db_port"]
            assert(int(self.port) > 1024 and int(self.port) <= 65535)
        except Exception as e:
            self.logger.debug(whoami() + str(e) + ", setting port to default 37703")
            self.port = "37703"
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + self.port)
        self.lock = threading.Lock()
        self.terminated = False

    def run(self):
        loc0 = locals()["self"]
        pw0 = getattr(loc0, "pwdb")
        while not self.terminated:
            # get command for pwdb
            try:
                funcstr, args0, kwargs0 = self.socket.recv_pyobj()
            except Exception as e:
                self.logger.debug(whoami() + str(e))
            # call pwdb.<funcstr>
            ret = eval("pw0." + funcstr + "(*args0, **kwargs0)")
            # send result
            try:
                self.socket.send_pyobj(ret)
            except Exception as e:
                self.logger.debug(whoami() + str(e))


def wrapper_main(cfg, dirs, logger):
    sh = Sighandler_gpww(logger)
    signal.signal(signal.SIGINT, sh.sighandler_gpww)
    signal.signal(signal.SIGTERM, sh.sighandler_gpww)

    pwwt = PwWrapperThread(cfg, dirs, logger)
    pwwt.start()
    while not TERMINATED:
        time.sleep(1)
    # pwwt.terminated = True
    # pwwt.join()
