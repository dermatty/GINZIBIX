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
import zmq
import threading
from threading import Thread
import psutil
import datetime

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
        self.logger.warning("signalhandler: closing pewee.db")
        self.pwdb.db_close()
        self.logger.info("Ginzibix terminated!")
        sys.exit()


class GUI_Drawer:

    def draw(self, data, pwdb_msg, server_config, threads):
        if not data or not pwdb_msg or not server_config or not threads:
            return
        bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
        avgmiblist = avgmiblist00
        max_mem_needed = 0
        bytescount0 = bytescount00
        bytescount0 += 0.00001
        overall_size += 0.00001
        availmem0 = availmem00
        # get Mib downloaded
        if len(avgmiblist) > 50:
            del avgmiblist[0]
        if len(avgmiblist) > 10:
            avgmib_dic = {}
            for (server_name, _, _, _, _, _, _, _, _) in server_config:
                bytescountlist = [bytescount for (_, bytescount, download_server0) in avgmiblist if server_name == download_server0]
                if len(bytescountlist) > 2:
                    avgmib_db = sum(bytescountlist)
                    avgmib_mint = min([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    avgmib_maxt = max([tt for (tt, _, download_server0) in avgmiblist if server_name == download_server0])
                    # print(avgmib_maxt, avgmib_mint)
                    avgmib_dic[server_name] = (avgmib_db / (avgmib_maxt - avgmib_mint)) / (1024 * 1024) * 8
                else:
                    avgmib_dic[server_name] = 0
            for server_name, avgmib in avgmib_dic.items():
                mem_needed = ((psutil.virtual_memory()[0] - psutil.virtual_memory()[1]) - availmem0) / (1024 * 1024 * 1024)
                if mem_needed > max_mem_needed:
                    max_mem_needed = mem_needed
        # set in all threads servers alive timestamps
        # check if server is longer off > 120 sec, if yes, kill thread & stop server
        if nzbname:
            print("--> Downloading " + nzbname)
        else:
            print("--> Downloading paused!" + " " * 30)
        try:
            print("MBit/sec.: " + str([sn + ": " + str(int(av)) + "  " for sn, av in avgmib_dic.items()]) + " max. mem_needed: "
                  + "{0:.3f}".format(max_mem_needed) + " GB                 ")
        except UnboundLocalError:
            print("MBit/sec.: --- max. mem_needed: " + str(max_mem_needed) + " GB                ")
        gbdown0 = 0
        mbitsec0 = 0
        for t_bytesdownloaded, t_last_timestamp, t_idn in threads:
            # for k, (t, last_timestamp) in enumerate(threads):
            gbdown = t_bytesdownloaded / (1024 * 1024 * 1024)
            gbdown0 += gbdown
            gbdown_str = "{0:.3f}".format(gbdown)
            mbitsec = (t_bytesdownloaded / (time.time() - t_last_timestamp)) / (1024 * 1024) * 8
            mbitsec0 += mbitsec
            mbitsec_str = "{0:.1f}".format(mbitsec)
            print(t_idn + ": Total - " + gbdown_str + " GB" + " | MBit/sec. - " + mbitsec_str + "                        ")
        print("-" * 60)
        gbdown0 += already_downloaded_size
        gbdown0_str = "{0:.3f}".format(gbdown0)
        perc0 = gbdown0 / overall_size
        if perc0 > 1:
            perc00 = 1
        else:
            perc00 = perc0
        print(gbdown0_str + " GiB (" + "{0:.1f}".format(perc00 * 100) + "%) of total "
              + "{0:.2f}".format(overall_size) + " GiB | MBit/sec. - " + "{0:.1f}".format(mbitsec0) + " " * 10)
        for key, item in filetypecounter00.items():
            print(key + ": " + str(item["counter"]) + "/" + str(item["max"]) + ", ", end="")
        if nzbname:
            trailing_spaces = " " * 10
        else:
            trailing_spaces = " " * 70
        print("Health: {0:.4f}".format(article_health * 100) + "%" + trailing_spaces)
        if mbitsec0 > 0 and perc0 < 1:
            eta = ((overall_size - gbdown0) * 1024)/(mbitsec0 / 8)
            print("Eta: " + str(datetime.timedelta(seconds=int(eta))) + " " * 30)
        else:
            print("Eta: - (done!)" + " " * 40)
        print()
        msg0 = pwdb_msg
        if msg0:
            print(" " * 120)
            sys.stdout.write("\033[F")
            print(msg0[-1][:120])
        for _ in range(len(threads) + 8):
            sys.stdout.write("\033[F")


class GUI_ConnectorMain(Thread):

    def __init__(self, lock, logger, port="36601"):
        Thread.__init__(self)
        self.daemon = True
        self.context = zmq.Context()
        self.host = "127.0.0.1"
        self.port = port
        self.lock = lock
        self.data = None
        self.nzbname = None

        self.socket = self.context.socket(zmq.REQ)
        self.logger = logger
        self.gui_drawer = GUI_Drawer()

    def set_data(self, data):
        with self.lock:
            bytescount00, availmem00, avgmiblist00, filetypecounter00, nzbname, article_health, overall_size, already_downloaded_size = data
            self.data, self.pwdb_msg = data
            self.nzbname = nzbname

    def get_data(self):
        ret0 = ("NOOK", None)
        with self.lock:
            try:
                ret0 = (self.data, self.pwdb_msg)
            except Exception as e:
                self.logger.error("GUI_Connector: " + str(e))
        return ret0

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
                # self.set_data((data, pwdb_msg))
                self.gui_drawer.draw(data, pwdb_msg, server_config, threads)
            except Exception as e:
                self.logger.error("GUI_ConnectorMain: " + str(e))
            time.sleep(1)


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

    progstr = "ginzibix 0.1-alpha, binary usenet downloader"
    print("Welcome to " + progstr)
    print("Press Ctrl-C to quit")
    print("-" * 60)

    logger.info("-" * 80)
    logger.info("starting " + progstr)
    logger.info("-" * 80)

    # connection threads
 
    # start guiconnector
    lock = threading.Lock()
    guipoller = GUI_ConnectorMain(lock, logger, port="36601")
    guipoller.start()

    # start main mp
    mpp_main = mp.Process(target=lib.ginzi_main, args=(cfg, pwdb, logger, ))
    mpp_main.start()
    sh.mpp_main = mpp_main

    while True:
        time.sleep(1)

    logger.warning("### EXITED GINZIBIX ###")

    sys.exit()
