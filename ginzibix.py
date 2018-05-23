#!/home/stephan/.virtualenvs/nntp/bin/python

import time
import sys
from os.path import expanduser
import configparser
import signal
import multiprocessing as mp
import logging
import logging.handlers
import lib
import queue
import threading

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


class ConnectionThreads:
    def __init__(self, cfg, articlequeue, resultqueue, logger):
        self.cfg = cfg
        self.logger = logger
        self.threads = []
        self.lock = threading.Lock()
        self.articlequeue = articlequeue
        self.resultqueue = resultqueue
        self.servers = None

    def init_servers(self):
        self.servers = lib.Servers(self.cfg, self.logger)
        self.level_servers = self.servers.level_servers
        self.all_connections = self.servers.all_connections

    def start_threads(self):
        if not self.threads:
            self.logger.debug("Starting download threads")
            self.init_servers()
            for sn, scon, _, _ in self.all_connections:
                t = lib.ConnectionWorker(self.lock, (sn, scon), self.articlequeue, self.resultqueue, self.servers, self.logger)
                self.threads.append((t, time.time()))
                t.start()
        else:
            self.logger.debug("Threads already started")

    def reset_timestamps(self):
        for t, _ in self.threads:
            t.last_timestamp = time.time()

    def reset_timestamps_bdl(self):
        if self.threads:
            for t, _ in self.threads:
                t.bytesdownloaded = 0
                t.last_timestamp = 0


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
    ct = ConnectionThreads(cfg, articlequeue, resultqueue, logger)

    # init sighandler
    mpp_pid = {"nzbparser": None, "decoder": None, "renamer": None, "verifier": None, "unrarer": None}
    sighandler = lib.SigHandler(ct.threads, pwdb, mp_work_queue, mpp_pid, logger)
    signal.signal(signal.SIGINT, sighandler.signalhandler)
    signal.signal(signal.SIGTERM, sighandler.signalhandler)

    progstr = "ginzibix 0.1-alpha, binary usenet downloader"
    print("Welcome to " + progstr)
    print("Press Ctrl-C to quit")
    print("-" * 60)

    logger.info("-" * 80)
    logger.info("starting " + progstr)
    logger.info("-" * 80)

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, pwdb, sighandler, ct, mpp_pid, mp_work_queue, logger, ))
    mpp_main.start()
    mpp_main_pid = mpp_main.pid

    while True:
        time.sleep(1)

    logger.warning("### EXITED GINZIBIX ###")

    sys.exit()
